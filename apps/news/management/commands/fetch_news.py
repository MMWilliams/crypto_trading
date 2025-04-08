from django.core.management.base import BaseCommand, CommandError
import requests
from django.utils.dateparse import parse_datetime
from firebase_admin import firestore
from apps.openai_integration.services import summarize_article
import os
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from celery.utils.log import get_task_logger
import time
from nltk.sentiment.vader import SentimentIntensityAnalyzer

logger = get_task_logger(__name__)

class Command(BaseCommand):
    help = 'Fetches cryptocurrency news, analyzes sentiment, counts events, and stores in Firestore'

    def __init__(self):
        super().__init__()
        self.db = firestore.client()
        self.NEWS_API_KEY = os.getenv("NEWS_API_KEY")
        self.NEWS_API_URL = 'https://newsapi.org/v2/everything'
        self.sentiment_analyzer = SentimentIntensityAnalyzer()
        self.relevant_coins = []  # Initialize relevant_coins
        self.market_types = ['crypto','forex']  #  'forex' removed as it's not related to crypto
    
    def get_relevant_coins_from_market_data(self) -> List[str]:
        """Get relevant coins from raw_market_data, removing prefixes like 'X:', 'C:', etc."""
        try:
            one_day_ago = int((datetime.now() - timedelta(days=1)).timestamp())
            query = (self.db.collection('raw_market_data')
                     .where('timestamp', '>=', one_day_ago))

            docs = query.stream()
            symbols = set()
            for doc in docs:
                data = doc.to_dict()
                symbol = data['symbol']

                # Remove any prefix ending with a colon
                if ':' in symbol:
                    symbol = symbol.split(':', 1)[1]

                symbols.add(symbol)

            relevant_coins = list(symbols)
            logger.info(f"Found {len(relevant_coins)} relevant coins in market data")
            return relevant_coins

        except Exception as e:
            logger.error(f"Error getting relevant coins from market data: {e}")
            return []

    def get_currencies(self) -> List[str]:
        """Get currency symbols from fetched market data or relevant coins list."""

        if self.relevant_coins:
            logger.info(f"Using {len(self.relevant_coins)} pre-loaded relevant coins")
            return self.relevant_coins

        if not self.market_types:  # Ensure market types are defined
            raise ValueError("Market types not defined.")

        all_symbols = set()  # Use a set for efficient unique symbol collection

        for market_type in self.market_types:
            market_data = self.fetch_market_data(market_type)  # Fetch market data for each type
            if market_data:
                for data_item in market_data:  # Extract symbols
                    all_symbols.add(data_item['symbol'])

        symbols_list = list(all_symbols)

        if not symbols_list:
            logger.warning("No currency symbols found in market data. Using defaults.")
            return ['BTC', 'ETH']  # Provide default symbols

        logger.info(f"Found {len(symbols_list)} currency symbols in market data.")
        return symbols_list

    def build_query_string(self, currencies: List[str]) -> str:
        """Build query string for relevant currencies."""
        if not currencies:
            return ""  # Return empty if nothing to process
        query_string = ' OR '.join(f'"{currency}"' for currency in currencies)
        return f'({query_string}) AND (crypto OR cryptocurrency OR blockchain)'

    def analyze_sentiment(self, text: str) -> float:
        """Perform sentiment analysis on text."""
        scores = self.sentiment_analyzer.polarity_scores(text)
        return scores['compound']  # Use compound score (-1 to 1)

    def extract_events(self, text: str, currencies: List[str]) -> int:
         """Count occurrences of events related to specified currencies."""
         # Basic keyword matching (can be improved with NER and relationship extraction)
         count = 0
         text_lower = text.lower()  # Convert text to lowercase once
         for currency in currencies:
             count += text_lower.count(currency.lower())  # Case-insensitive counting
         return count
    def build_query_string(self, currencies: List[str]) -> str:
        """Build query string for a group of currencies"""
        query_string = ' OR '.join(f'"{currency}"' for currency in currencies)
        return f'({query_string}) AND (crypto OR cryptocurrency OR blockchain)'

    def generate_document_id(self, url: str) -> str:
        """Generate a safe document ID from URL"""
        return hashlib.md5(url.encode()).hexdigest()

    def format_article_data(self, article: Dict, currencies: List[str]) -> Dict:
        """Format article data for Firestore storage with sentiment and event analysis"""
        # Get the text content to analyze
        content_to_analyze = ' '.join(filter(None, [
            article.get('title', ''),
            article.get('description', ''),
            article.get('content', '')
        ]))

        # Perform sentiment analysis and event counting
        sentiment_score = self.analyze_sentiment(content_to_analyze)
        event_count = self.extract_events(content_to_analyze, currencies)

        return {
            'title': article.get('title'),
            'description': article.get('description'),
            'content': article.get('content'),
            'url': article.get('url'),
            'url_to_image': article.get('urlToImage'),
            'published_at': article.get('publishedAt'),
            'source': article['source'].get('name'),
            'created_at': firestore.SERVER_TIMESTAMP,
            'updated_at': firestore.SERVER_TIMESTAMP,
            'currencies_mentioned': currencies,
            'article_type': 'crypto',
            'sentiment_score': sentiment_score,
            'event_count': event_count
        }

    def fetch_articles_for_currencies(self, currencies: List[str]) -> List[Dict]:
        """Fetch articles for a specific group of currencies"""
        query_string = self.build_query_string(currencies)
        logger.info(f"Fetching news for query: {query_string}")
        
        params = {
            'q': query_string,
            'language': 'en',
            'sortBy': 'publishedAt',
            'apiKey': self.NEWS_API_KEY,
            'pageSize': 20,
        }
        
        response = requests.get(self.NEWS_API_URL, params=params)
        data = response.json()

        if data['status'] != 'ok':
            logger.error(f"API Error for currencies {currencies}: {data.get('message')}")
            return []

        return data.get('articles', [])

    def process_articles_batch(self, articles: List[Dict], currencies: List[str]) -> Tuple[int, int]:
        """Process and store a batch of articles"""
        batch = self.db.batch()
        articles_processed = 0
        articles_with_summaries = 0
        
        for article in articles:
            try:
                if not article.get('url'):
                    continue
        
                doc_id = self.generate_document_id(article['url'])
                article_data = self.format_article_data(article, currencies)
                
                if article_data['content'] or article_data['description']:
                    try:
                        summary = summarize_article(
                            article_data['content'] or article_data['description'] or ""
                        )
                        if summary:
                            article_data['summary'] = summary
                            articles_with_summaries += 1
                    except Exception as e:
                        logger.error(f"Error generating summary: {str(e)}")
                else:
                    logger.error(f"Error generating summary: no content or description")

                # Log sentiment and event analysis results
                logger.info(f"Article {doc_id} analysis - Sentiment: {article_data['sentiment_score']:.2f}, "
                          f"Events: {article_data['event_count']}")

                doc_ref = self.db.collection('news_articles').document(doc_id)
                batch.set(doc_ref, article_data, merge=True)
                articles_processed += 1

            except Exception as e:
                logger.error(f'Error processing article: {str(e)}')
                continue

        if articles_processed > 0:
            batch.commit()

        return articles_processed, articles_with_summaries

    def handle(self, *args, **options):
        """Django command handler"""

        if not self.NEWS_API_KEY:
            raise CommandError('NEWS_API_KEY not configured.')

        try:
            # Initialize database connection first
            self.db = firestore.client()  # Initialize within handle
            if not self.db:
                raise ValueError("Failed to initialize database connection")

            # Fetch relevant coins from raw_market_data
            self.relevant_coins = self.get_relevant_coins_from_market_data()

            currencies = self.get_currencies()  # Use relevant coins or market data
            if not currencies:
                logger.warning("No relevant or active currencies found. Exiting.")
                return

            total_processed = 0
            total_with_summaries = 0

            chunk_size = 10  # Process currencies in smaller chunks to avoid long query strings
            for i in range(0, len(currencies), chunk_size):
                currency_chunk = currencies[i:i + chunk_size]
                logger.info(f"Processing currencies chunk {i // chunk_size + 1}: {', '.join(currency_chunk)}")

                try:
                    articles = self.fetch_articles_for_currencies(currency_chunk)
                    if not articles:
                        logger.warning(f"No articles found for chunk {i // chunk_size + 1}. Skipping.")
                        continue  # Skip to the next chunk if no articles are found
                        
                    processed, with_summaries = self.process_articles_batch(articles, currency_chunk)

                    total_processed += processed
                    total_with_summaries += with_summaries

                    self.stdout.write(
                        self.style.SUCCESS(
                            f'Processed chunk {i // chunk_size + 1}: {processed} articles '
                            f'({with_summaries} with summaries)'
                        )
                    )

                    if i + chunk_size < len(currencies):
                        time.sleep(2)  # Increased sleep time for rate limiting
                except requests.exceptions.RequestException as e: # specific exception for API errors
                    logger.error(f"API Error processing chunk {currency_chunk}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing chunk {currency_chunk}: {str(e)}")
                    continue

            self.stdout.write(
                self.style.SUCCESS(
                    f'Successfully processed a total of {total_processed} articles '
                    f'({total_with_summaries} with summaries)'
                )
            )

        except Exception as e:
            logger.exception(f'Unexpected error: {e}')  # Log with traceback for unexpected errors
            raise CommandError(f'News fetching failed: {e}')