import os
from flask import Flask, jsonify, render_template, request
from flask_socketio import SocketIO, emit
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, timedelta
import random # For mock data
from dotenv import load_dotenv
import threading
import time

load_dotenv() # Load environment variables from .env file

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key'
socketio = SocketIO(app, cors_allowed_origins="*")

# MongoDB Connection - Updated for streaming collections
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin@localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "amazon_reviews")

# Multiple collections for different data sources
STREAMING_COLLECTION = "streaming_reviews"  # Spark streaming data
KAFKA_COLLECTION = "kafka_reviews"          # Direct Kafka consumer data
BATCH_COLLECTION = "batch_reviews"          # Original batch processing data

try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Collections for different data sources
    streaming_collection = db[STREAMING_COLLECTION]
    kafka_collection = db[KAFKA_COLLECTION]
    batch_collection = db[BATCH_COLLECTION]
    
    print("Successfully connected to MongoDB.")
    print(f"Available collections: {STREAMING_COLLECTION}, {KAFKA_COLLECTION}, {BATCH_COLLECTION}")
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")
    streaming_collection = None
    kafka_collection = None
    batch_collection = None

# Cache the active collection to avoid repeated lookups
_active_collection = None
_last_collection_check = None

def get_active_collection():
    """Get the most active collection with recent data"""
    global _active_collection, _last_collection_check
    
    # Cache for 30 seconds to avoid repeated lookups
    if _active_collection is not None and _last_collection_check is not None:
        if datetime.utcnow() - _last_collection_check < timedelta(seconds=30):
            return _active_collection
    
    # Prioritize kafka_collection since that's where streaming data goes
    collections = [kafka_collection, streaming_collection, batch_collection]
    
    for collection in collections:
        if collection is not None:
            try:
                # Check if collection has any documents
                count = collection.count_documents({})
                if count > 0:
                    print(f"Found active collection: {collection.name} with {count} documents")
                    _active_collection = collection
                    _last_collection_check = datetime.utcnow()
                    return collection
            except Exception as e:
                continue
    
    _active_collection = None
    _last_collection_check = datetime.utcnow()
    return None

# --- Mock Data Generation (if needed) ---
def get_mock_review():
    sentiments = ["Positive", "Neutral", "Negative"]
    asins = ["B00004Y2UT", "B00005Y2UX", "B00006Y2UZ", "B00007Y2UA", "B00008Y2UB"]
    return {
        "asin": random.choice(asins),
        "reviewText": "This is a mock review text. " + " ".join(random.choices(["great", "bad", "ok", "product", "item", "service"], k=5)),
        "sentiment": random.choice(sentiments),
        "confidence": round(random.uniform(0.5, 1.0), 2),
        "timestamp": datetime.utcnow() - timedelta(seconds=random.randint(0, 300))
    }

# --- Routes ---
@app.route('/')
def index():
    return render_template('index.html')

# --- API Endpoints for Online Panel ---
@app.route('/api/latest_review')
def latest_review():
    # Prioritize kafka_collection since that's where streaming data goes with processed_at timestamps
    for collection in [kafka_collection, streaming_collection, batch_collection]:
        if collection is not None:
            try:
                latest_reviews = []
                
                # For kafka_collection, prioritize processed_at field
                if collection == kafka_collection:
                    latest_reviews = list(collection.find().sort([("processed_at", -1)]).limit(10))
                    if not latest_reviews:
                        # Fallback to other timestamp fields
                        latest_reviews = list(collection.find().sort([("processing_timestamp", -1)]).limit(10)) or \
                                        list(collection.find().sort([("timestamp", -1)]).limit(10)) or \
                                        list(collection.find().sort([("_id", -1)]).limit(10))
                else:
                    # For other collections, use the original logic
                    latest_reviews = list(collection.find().sort([("processing_timestamp", -1)]).limit(10)) or \
                                    list(collection.find().sort([("processed_at", -1)]).limit(10)) or \
                                    list(collection.find().sort([("timestamp", -1)]).limit(10)) or \
                                    list(collection.find().sort([("_id", -1)]).limit(10))
                
                if latest_reviews:
                    # Process each review
                    processed_reviews = []
                    for review in latest_reviews:
                        review["_id"] = str(review["_id"])
                        
                        # Handle different timestamp fields - prioritize processed_at for kafka_collection
                        if collection == kafka_collection and "processed_at" in review:
                            # processed_at is already a string in the format '2025-05-24T11:05:14.155870'
                            review["timestamp"] = review["processed_at"]
                        elif "processing_timestamp" in review:
                            review["timestamp"] = review["processing_timestamp"].isoformat() if hasattr(review["processing_timestamp"], 'isoformat') else str(review["processing_timestamp"])
                        elif "processed_at" in review:
                            review["timestamp"] = review["processed_at"]
                        elif "timestamp" in review and hasattr(review["timestamp"], 'isoformat'):
                            review["timestamp"] = review["timestamp"].isoformat()
                        else:
                            review["timestamp"] = datetime.utcnow().isoformat()
                        
                        processed_reviews.append(review)
                    
                    print(f"Found {len(processed_reviews)} latest reviews from {collection.name}")
                    return jsonify(processed_reviews)
            except Exception as e:
                print(f"Error fetching latest reviews from {collection.name}: {e}")
                continue
    
    # Fallback to mock data - return 10 mock reviews
    mock_reviews = []
    for _ in range(10):
        mock_data = get_mock_review()
        mock_data["_id"] = str(ObjectId())
        mock_data["timestamp"] = mock_data["timestamp"].isoformat()
        mock_reviews.append(mock_data)
    return jsonify(mock_reviews)

@app.route('/api/live_sentiment_summary')
def live_sentiment_summary():
    # Simulates sentiment count in the last 5 minutes
    labels = ["Positive", "Neutral", "Negative"]
    data = [random.randint(0, 20) for _ in labels] # Mock data fallback
    
    active_collection = get_active_collection()
    if active_collection is not None:
        try:
            five_minutes_ago = datetime.utcnow() - timedelta(minutes=5)
            pipeline = [
                {"$match": {"timestamp": {"$gte": five_minutes_ago}}},
                {"$group": {"_id": "$sentiment", "count": {"$sum": 1}}},
                {"$sort": {"_id": 1}}
            ]
            results = list(active_collection.aggregate(pipeline))
            # Ensure all sentiment categories are present, even if count is 0
            sentiment_counts = {label: 0 for label in labels}
            for r in results:
                if r["_id"] in sentiment_counts:
                    sentiment_counts[r["_id"]] = r["count"]
            data = [sentiment_counts[label] for label in labels]
            return jsonify({"labels": labels, "data": data})
        except Exception as e:
            print(f"Error fetching live sentiment summary from DB: {e}")
            # Fallback to mock data if DB query fails
            return jsonify({"labels": labels, "data": [random.randint(0, 20) for _ in labels]})

    return jsonify({"labels": labels, "data": data})


# --- API Endpoints for Offline Analysis Panel ---
@app.route('/api/sentiment_over_time')
def sentiment_over_time():
    active_collection = get_active_collection()
    if active_collection is None:
        return jsonify({"error": "Database not connected"}), 500
        
    try:
        # Get query parameters
        target_asin = request.args.get('asin')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        pipeline = []
        match_query = {}
        
        # Add ASIN filter
        if target_asin:
            match_query["asin"] = target_asin
        
        # Add date range filter
        if start_date or end_date:
            date_filter = {}
            if start_date:
                try:
                    start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                    date_filter["$gte"] = start_dt
                except:
                    pass
            if end_date:
                try:
                    end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                    date_filter["$lte"] = end_dt
                except:
                    pass
            if date_filter:
                match_query["timestamp"] = date_filter

        if match_query:
            pipeline.append({"$match": match_query})

        # Group by date and sentiment
        pipeline.extend([
            {
                "$addFields": {
                    "dateOnly": {
                        "$dateToString": {
                            "format": "%Y-%m-%d", 
                            "date": "$timestamp",
                            "timezone": "UTC"
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "date": "$dateOnly",
                        "sentiment": "$sentiment"
                    },
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"_id.date": 1, "_id.sentiment": 1}}
        ])
        
        results = list(active_collection.aggregate(pipeline))
        
        # Process data for chart
        data_for_chart = {}
        for r in results:
            date = r["_id"]["date"]
            sentiment = r["_id"]["sentiment"]
            count = r["count"]
            if date not in data_for_chart:
                data_for_chart[date] = {"Positive": 0, "Neutral": 0, "Negative": 0}
            if sentiment in data_for_chart[date]:
                 data_for_chart[date][sentiment] = count
        
        labels = sorted(data_for_chart.keys())
        datasets = {
            "Positive": [data_for_chart[date].get("Positive", 0) for date in labels],
            "Neutral": [data_for_chart[date].get("Neutral", 0) for date in labels],
            "Negative": [data_for_chart[date].get("Negative", 0) for date in labels],
        }
        return jsonify({"labels": labels, "datasets": datasets})

    except Exception as e:
        print(f"Error in sentiment_over_time: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/product_analysis/<asin>')
def product_analysis(asin):
    active_collection = get_active_collection()
    if active_collection is None:
        return jsonify({"error": "Database not connected"}), 500

    try:
        # Check if product exists
        product_exists = active_collection.count_documents({"asin": asin})
        if product_exists == 0:
            return jsonify({"error": f"No reviews found for ASIN: {asin}"}), 404

        # Pie chart: % Positive / Neutral / Negative for the ASIN
        pipeline_pie = [
            {"$match": {"asin": asin}},
            {"$group": {"_id": "$sentiment", "count": {"$sum": 1}}}
        ]
        pie_data_raw = list(active_collection.aggregate(pipeline_pie))
        
        sentiments = ["Positive", "Neutral", "Negative"]
        pie_counts = {s: 0 for s in sentiments}
        for item in pie_data_raw:
            if item["_id"] in pie_counts:
                pie_counts[item["_id"]] = item["count"]
        
        pie_chart_data = {
            "labels": sentiments,
            "data": [pie_counts[s] for s in sentiments]
        }

        # Review count over time for that ASIN (e.g., daily) - sorted by date
        pipeline_temporal = [
            {"$match": {"asin": asin}},
            {
                "$addFields": {
                    "dateOnly": {
                        "$dateToString": {
                            "format": "%Y-%m-%d", 
                            "date": "$timestamp",
                            "timezone": "UTC"
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$dateOnly",
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"_id": 1}}
        ]
        temporal_data_raw = list(active_collection.aggregate(pipeline_temporal))
        temporal_chart_data = {
            "labels": [item["_id"] for item in temporal_data_raw],
            "data": [item["count"] for item in temporal_data_raw]
        }
        
        # Get recent reviews for this product (latest first)
        recent_reviews_pipeline = [
            {"$match": {"asin": asin}},
            {"$sort": {"timestamp": -1}},
            {"$limit": 10},
            {
                "$project": {
                    "reviewText": 1,
                    "sentiment": 1,
                    "confidence": 1,
                    "timestamp": 1,
                    "overall": 1
                }
            }
        ]
        recent_reviews_raw = list(active_collection.aggregate(recent_reviews_pipeline))
        
        # Format recent reviews
        recent_reviews = []
        for review in recent_reviews_raw:
            review["_id"] = str(review["_id"])
            if "timestamp" in review and hasattr(review["timestamp"], 'isoformat'):
                review["timestamp"] = review["timestamp"].isoformat()
            # Truncate review text for display
            if "reviewText" in review and review["reviewText"]:
                review["reviewTextShort"] = review["reviewText"][:100] + "..." if len(review["reviewText"]) > 100 else review["reviewText"]
            else:
                review["reviewTextShort"] = "No review text available"
            recent_reviews.append(review)

        # Get confidence statistics
        confidence_pipeline = [
            {"$match": {"asin": asin, "confidence": {"$exists": True}}},
            {
                "$group": {
                    "_id": None,
                    "avg_confidence": {"$avg": "$confidence"},
                    "min_confidence": {"$min": "$confidence"},
                    "max_confidence": {"$max": "$confidence"}
                }
            }
        ]
        confidence_stats = list(active_collection.aggregate(confidence_pipeline))
        confidence_data = confidence_stats[0] if confidence_stats else {
            "avg_confidence": 0.85,
            "min_confidence": 0.5,
            "max_confidence": 1.0
        }

        return jsonify({
            "asin": asin,
            "total_reviews": product_exists,
            "sentiment_distribution": pie_chart_data,
            "review_count_over_time": temporal_chart_data,
            "recent_reviews": recent_reviews,
            "confidence_stats": {
                "average": round(confidence_data.get("avg_confidence", 0.85), 2),
                "minimum": round(confidence_data.get("min_confidence", 0.5), 2),
                "maximum": round(confidence_data.get("max_confidence", 1.0), 2)
            }
        })
    except Exception as e:
        print(f"Error in product_analysis for {asin}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/global_stats')
def global_stats():
    active_collection = get_active_collection()
    if active_collection is None:
        return jsonify({"error": "Database not connected"}), 500
    try:
        total_reviews = active_collection.count_documents({})

        sentiment_dist_pipeline = [
            {"$group": {"_id": "$sentiment", "count": {"$sum": 1}}}
        ]
        sentiment_dist_raw = list(active_collection.aggregate(sentiment_dist_pipeline))
        sentiment_distribution = {item["_id"]: item["count"] for item in sentiment_dist_raw}

        top_reviewed_pipeline = [
            {"$group": {"_id": "$asin", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        top_reviewed_products = list(active_collection.aggregate(top_reviewed_pipeline))

        top_negative_pipeline = [
            {"$match": {"sentiment": "Negative"}},
            {"$group": {"_id": "$asin", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        top_negative_products = list(active_collection.aggregate(top_negative_pipeline))

        return jsonify({
            "total_reviews_processed": total_reviews,
            "sentiment_distribution_all": sentiment_distribution,
            "top_5_most_reviewed_products": top_reviewed_products,
            "top_5_most_negative_products": top_negative_products
        })
    except Exception as e:
        print(f"Error in global_stats: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/streaming_stats')
def streaming_stats():
    """Real-time streaming statistics"""
    active_collection = get_active_collection()
    if active_collection is None:
        # Return mock data if no collection available
        return jsonify({
            "total_processed": random.randint(1000, 5000),
            "processing_rate": random.randint(10, 50),
            "avg_confidence": round(random.uniform(0.7, 0.95), 2),
            "last_updated": datetime.utcnow().isoformat()
        })
    
    try:
        # Get recent statistics from the last hour
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        
        # Total processed in last hour
        total_recent = active_collection.count_documents({
            "timestamp": {"$gte": one_hour_ago}
        })
        
        # Processing rate (reviews per minute in last 10 minutes)
        ten_minutes_ago = datetime.utcnow() - timedelta(minutes=10)
        recent_count = active_collection.count_documents({
            "timestamp": {"$gte": ten_minutes_ago}
        })
        processing_rate = round(recent_count / 10, 1) if recent_count > 0 else 0
        
        # Average confidence score
        pipeline = [
            {"$match": {"timestamp": {"$gte": one_hour_ago}, "confidence": {"$exists": True}}},
            {"$group": {"_id": None, "avg_confidence": {"$avg": "$confidence"}}}
        ]
        confidence_result = list(active_collection.aggregate(pipeline))
        avg_confidence = round(confidence_result[0]["avg_confidence"], 2) if confidence_result else 0.85
        
        return jsonify({
            "total_processed": total_recent,
            "processing_rate": processing_rate,
            "avg_confidence": avg_confidence,
            "last_updated": datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        print(f"Error in streaming_stats: {e}")
        # Fallback to mock data
        return jsonify({
            "total_processed": random.randint(100, 1000),
            "processing_rate": random.randint(5, 25),
            "avg_confidence": round(random.uniform(0.7, 0.95), 2),
            "last_updated": datetime.utcnow().isoformat()
        })

@socketio.on('connect')
def handle_connect():
    print('Client connected')
    emit('status', {'msg': 'Connected to dashboard'})

@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')

@app.route('/api/recent_reviews')
def recent_reviews():
    """Get recent reviews for the dashboard"""
    active_collection = get_active_collection()
    if active_collection is None:
        # Return mock data if no collection available
        mock_reviews = []
        for _ in range(10):
            review = get_mock_review()
            review["_id"] = str(ObjectId())
            review["timestamp"] = review["timestamp"].isoformat()
            mock_reviews.append(review)
        return jsonify(mock_reviews)
    
    try:
        # Get last 10 reviews - sorted by timestamp descending (latest first)
        recent_reviews_list = list(active_collection.find().sort("timestamp", -1).limit(10))
        
        # Convert ObjectId to string and handle timestamps
        for review in recent_reviews_list:
            review["_id"] = str(review["_id"])
            if "timestamp" in review and hasattr(review["timestamp"], 'isoformat'):
                review["timestamp"] = review["timestamp"].isoformat()
            elif "processing_timestamp" in review:
                review["timestamp"] = review["processing_timestamp"].isoformat() if hasattr(review["processing_timestamp"], 'isoformat') else str(review["processing_timestamp"])
            elif "processed_at" in review:
                review["timestamp"] = review["processed_at"]
            else:
                review["timestamp"] = datetime.utcnow().isoformat()
        
        return jsonify(recent_reviews_list)
        
    except Exception as e:
        print(f"Error in recent_reviews: {e}")
        # Fallback to mock data
        mock_reviews = []
        for _ in range(10):
            review = get_mock_review()
            review["_id"] = str(ObjectId())
            review["timestamp"] = review["timestamp"].isoformat()
            mock_reviews.append(review)
        return jsonify(mock_reviews)

@app.route('/api/available_products')
def available_products():
    """Get list of available ASINs for the dropdown"""
    active_collection = get_active_collection()
    if active_collection is None:
        return jsonify(["B00004Y2UT", "B00005Y2UX", "B00006Y2UZ", "B00007Y2UA", "B00008Y2UB"])
    
    try:
        # Get unique ASINs with review counts
        pipeline = [
            {"$group": {"_id": "$asin", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 50}  # Limit to top 50 products
        ]
        results = list(active_collection.aggregate(pipeline))
        products = [{"asin": item["_id"], "review_count": item["count"]} for item in results]
        return jsonify(products)
        
    except Exception as e:
        print(f"Error in available_products: {e}")
        return jsonify(["B00004Y2UT", "B00005Y2UX", "B00006Y2UZ", "B00007Y2UA", "B00008Y2UB"])

def background_thread():
    """Send periodic updates to connected clients"""
    while True:
        time.sleep(5)  # Update every 5 seconds
        active_collection = get_active_collection()
        
        if active_collection is not None:
            try:
                # Get latest review - prioritize kafka_collection with processed_at
                latest = None
                if active_collection == kafka_collection:
                    latest = active_collection.find_one(sort=[("processed_at", -1)])
                
                if not latest:
                    # Fallback to other timestamp fields
                    latest = active_collection.find_one(sort=[("timestamp", -1)]) or \
                            active_collection.find_one(sort=[("processing_timestamp", -1)]) or \
                            active_collection.find_one(sort=[("_id", -1)])
                
                if latest:
                    latest["_id"] = str(latest["_id"])
                    if active_collection == kafka_collection and "processed_at" in latest:
                        latest["timestamp"] = latest["processed_at"]
                    elif "timestamp" in latest and hasattr(latest["timestamp"], 'isoformat'):
                        latest["timestamp"] = latest["timestamp"].isoformat()
                    elif "processing_timestamp" in latest:
                        latest["timestamp"] = latest["processing_timestamp"].isoformat() if hasattr(latest["processing_timestamp"], 'isoformat') else str(latest["processing_timestamp"])
                    elif "processed_at" in latest:
                        latest["timestamp"] = latest["processed_at"]
                    else:
                        latest["timestamp"] = datetime.utcnow().isoformat()
                    socketio.emit('new_review', latest)
                
                # Get live sentiment counts
                five_minutes_ago = datetime.utcnow() - timedelta(minutes=5)
                pipeline = [
                    {"$match": {"timestamp": {"$gte": five_minutes_ago}}},
                    {"$group": {"_id": "$sentiment", "count": {"$sum": 1}}}
                ]
                results = list(active_collection.aggregate(pipeline))
                sentiment_data = {"Positive": 0, "Neutral": 0, "Negative": 0}
                for r in results:
                    if r["_id"] in sentiment_data:
                        sentiment_data[r["_id"]] = r["count"]
                
                socketio.emit('sentiment_update', {
                    "labels": list(sentiment_data.keys()),
                    "data": list(sentiment_data.values())
                })
                
            except Exception as e:
                print(f"Error in background thread: {e}")
        else:
            # Send mock data if no collection
            mock_review = get_mock_review()
            mock_review["_id"] = str(ObjectId())
            mock_review["timestamp"] = mock_review["timestamp"].isoformat()
            socketio.emit('new_review', mock_review)
            
            mock_sentiment = {
                "labels": ["Positive", "Neutral", "Negative"],
                "data": [random.randint(0, 20) for _ in range(3)]
            }
            socketio.emit('sentiment_update', mock_sentiment)

# Start background thread
thread = threading.Thread(target=background_thread)
thread.daemon = True
thread.start()

if __name__ == '__main__':
    # For development, you might want to insert some mock data if DB is empty
    active_collection = get_active_collection()
    if active_collection is not None and active_collection.count_documents({}) == 0:
        print("MongoDB collection is empty. Inserting some mock data...")
        mock_docs = []
        for _ in range(50): # Insert 50 mock reviews
            review = get_mock_review()
            # Ensure timestamp is a datetime object for MongoDB
            if isinstance(review["timestamp"], str):
                review["timestamp"] = datetime.fromisoformat(review["timestamp"].replace("Z", "+00:00"))
            mock_docs.append(review)
        if mock_docs:
            active_collection.insert_many(mock_docs)
            print(f"Inserted {len(mock_docs)} mock documents.")

    socketio.run(app, debug=True, port=5001)
