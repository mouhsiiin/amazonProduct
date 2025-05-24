import pandas as pd
import numpy as np
import re
import json
import nltk
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from datetime import datetime

# Print current date and time in UTC
print(f"Current Date and Time (UTC): 2025-05-19 19:12:51")
print(f"Current User's Login: mouhsiiin")

# Download NLTK resources (only needed once)
print("Downloading NLTK resources...")
try:
    nltk.download('punkt', quiet=True)
    nltk.download('wordnet', quiet=True)
    nltk.download('stopwords', quiet=True)
except Exception as e:
    print(f"Error downloading NLTK resources: {e}")
    print("Continuing with available resources...")

# Initialize NLTK components
lemmatizer = WordNetLemmatizer()
stop_words = set(stopwords.words('english'))

# Function to clean and preprocess text using NLTK instead of spaCy
def clean_text(text):
    if not isinstance(text, str):
        return ""
    
    # Convert to lowercase
    text = text.lower()
    
    # Remove URLs, HTML tags, and special characters
    text = re.sub(r'http\S+|www\S+|https\S+', '', text)
    text = re.sub(r'<.*?>', '', text)
    text = re.sub(r'[^\w\s]', '', text)
    
    # Tokenize
    tokens = word_tokenize(text)
    
    # Lemmatization and stopword removal
    lemmatized_tokens = [lemmatizer.lemmatize(token) for token in tokens if token not in stop_words and token.isalpha()]
    
    return " ".join(lemmatized_tokens)

# Function to generate sentiment labels
def generate_label(rating):
    if rating < 3:
        return 0  # Negative
    elif rating == 3:
        return 1  # Neutral
    else:
        return 2  # Positive

# Load the dataset
print("Loading dataset...")
try:
    # For large JSON files, we'll read line by line
    reviews = []
    with open('Data.json', 'r') as f:
        for line in f:
            try:
                reviews.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    
    df = pd.DataFrame(reviews)
    print(f"Dataset loaded. Shape: {df.shape}")
except FileNotFoundError:
    print("Data.json file not found. Checking for alternative formats...")
    try:
        # Try loading as CSV in case format was converted
        df = pd.read_csv('Data.csv')
        print(f"Loaded Data.csv instead. Shape: {df.shape}")
    except FileNotFoundError:
        try:
            # Try loading as Excel
            df = pd.read_excel('Data.xlsx')
            print(f"Loaded Data.xlsx instead. Shape: {df.shape}")
        except FileNotFoundError:
            print("ERROR: Could not find data file (Data.json/csv/xlsx)")
            # Create a small sample dataset for demonstration
            print("Creating a sample dataset for demonstration...")
            sample_data = {
                'reviewText': [
                    "This product is amazing! I love it.",
                    "Not worth the money, terrible quality.",
                    "It's okay, nothing special about it.",
                    "Best purchase I've ever made, highly recommend!",
                    "Broke after one week. Very disappointed."
                ],
                'overall': [5, 1, 3, 5, 1]
            }
            df = pd.DataFrame(sample_data)
            print(f"Sample dataset created. Shape: {df.shape}")

# Check if required columns exist
required_columns = ['reviewText', 'overall']
missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    print(f"Warning: Missing columns: {missing_columns}")
    if 'review' in df.columns and 'reviewText' not in df.columns:
        df['reviewText'] = df['review']
    if 'rating' in df.columns and 'overall' not in df.columns:
        df['overall'] = df['rating']

# Keep only necessary columns and drop rows with missing values
df = df[required_columns].dropna()
print(f"Shape after dropping missing values: {df.shape}")

# Clean the review text
print("Cleaning text data...")
df['cleaned_text'] = df['reviewText'].apply(clean_text)

# Generate sentiment labels
print("Generating sentiment labels...")
df['sentiment'] = df['overall'].apply(generate_label)

# Print sentiment distribution
sentiment_counts = df['sentiment'].value_counts()
print("Sentiment distribution:")
for label, count in sentiment_counts.items():
    sentiment_name = {0: "Negative", 1: "Neutral", 2: "Positive"}[label]
    percentage = 100 * count / len(df)
    print(f"{sentiment_name}: {count} ({percentage:.2f}%)")

# Apply TF-IDF vectorization
print("Applying TF-IDF vectorization...")
vectorizer = TfidfVectorizer(max_features=10000)
X = vectorizer.fit_transform(df['cleaned_text'])
y = df['sentiment']

# Split into train, validation, and test sets
print("Splitting dataset...")
# First split: 80% train, 20% temp (validation + test)
X_train, X_temp, y_train, y_temp = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Second split: 50% validation, 50% test (10% each of total)
X_val, X_test, y_val, y_test = train_test_split(X_temp, y_temp, test_size=0.5, random_state=42, stratify=y_temp)

print(f"Training set shape: {X_train.shape}")
print(f"Validation set shape: {X_val.shape}")
print(f"Test set shape: {X_test.shape}")

# Save processed data
print("Saving processed data...")
import scipy.sparse as sp
import pickle

try:
    # Save vectorizer for future use
    with open('tfidf_vectorizer.pkl', 'wb') as f:
        pickle.dump(vectorizer, f)

    # Save TF-IDF matrices
    sp.save_npz('X_train.npz', X_train)
    sp.save_npz('X_val.npz', X_val)
    sp.save_npz('X_test.npz', X_test)

    # Save labels
    np.save('y_train.npy', y_train)
    np.save('y_val.npy', y_val)
    np.save('y_test.npy', y_test)
    
    print("Processing complete. Data is ready for model training.")
except Exception as e:
    print(f"Error saving processed data: {e}")
    print("Continuing without saving files...")
    print("Processing complete. Data is ready for model training in memory.")