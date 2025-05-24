document.addEventListener('DOMContentLoaded', function() {
    // Chart instances
    let liveSentimentChartInstance = null;
    let sentimentOverTimeChartInstance = null;
    let productSentimentPieChartInstance = null;
    let productReviewCountChartInstance = null;

    let selectedAsin = null;

    // --- Online Panel Functions ---
    function showLoading(elementId) {
        const element = document.getElementById(elementId);
        if (element) {
            element.innerHTML = '<div class="text-center loading">Loading...</div>';
        }
    }

    function fetchLatestReview() {
        showLoading('latest-review-body');
        fetch('/api/latest_review')
            .then(response => response.json())
            .then(data => {
                const tbody = document.getElementById('latest-review-body');
                tbody.innerHTML = ''; // Clear previous
                
                // Handle both single review (backward compatibility) and array of reviews
                const reviews = Array.isArray(data) ? data : [data];
                
                reviews.forEach(reviewData => {
                    const row = tbody.insertRow();
                    
                    let sentimentBadge = '';
                    if (reviewData.sentiment === 'Positive') {
                        sentimentBadge = `<span class="badge badge-success"><i class="fas fa-smile mr-1"></i>${reviewData.sentiment}</span>`;
                    } else if (reviewData.sentiment === 'Negative') {
                        sentimentBadge = `<span class="badge badge-danger"><i class="fas fa-frown mr-1"></i>${reviewData.sentiment}</span>`;
                    } else {
                        sentimentBadge = `<span class="badge badge-warning"><i class="fas fa-meh mr-1"></i>${reviewData.sentiment}</span>`;
                    }

                    // Format timestamp to show how recent it is
                    let timestampDisplay = 'N/A';
                    if (reviewData.timestamp) {
                        try {
                            // Handle different timestamp formats
                            let reviewDate;
                            if (reviewData.timestamp.includes('T')) {
                                // ISO format or similar
                                reviewDate = new Date(reviewData.timestamp);
                            } else {
                                // Try parsing as is
                                reviewDate = new Date(reviewData.timestamp);
                            }
                            
                            if (!isNaN(reviewDate.getTime())) {
                                const now = new Date();
                                const diffMinutes = Math.floor((now - reviewDate) / (1000 * 60));
                                
                                if (diffMinutes < 1) {
                                    timestampDisplay = `<span class="text-success"><i class="fas fa-circle mr-1" style="font-size: 8px;"></i>Just now</span>`;
                                } else if (diffMinutes < 60) {
                                    timestampDisplay = `<span class="text-info">${diffMinutes} min ago</span>`;
                                } else if (diffMinutes < 1440) { // less than 24 hours
                                    const hours = Math.floor(diffMinutes / 60);
                                    timestampDisplay = `<span class="text-warning">${hours}h ago</span>`;
                                } else {
                                    timestampDisplay = `<span class="text-muted">${reviewDate.toLocaleDateString()}</span>`;
                                }
                            } else {
                                timestampDisplay = `<span class="text-muted">${reviewData.timestamp}</span>`;
                            }
                        } catch (error) {
                            console.error('Error parsing timestamp:', reviewData.timestamp, error);
                            timestampDisplay = `<span class="text-muted">${reviewData.timestamp}</span>`;
                        }
                    }

                    row.insertCell().innerHTML = timestampDisplay;
                    row.insertCell().innerHTML = `<code>${reviewData.asin || 'N/A'}</code>`;
                    row.insertCell().innerHTML = sentimentBadge;
                    row.insertCell().textContent = reviewData.reviewText ? reviewData.reviewText.substring(0, 50) + '...' : 'N/A';
                    row.insertCell().innerHTML = `<span class="badge badge-info">${reviewData.confidence !== undefined ? reviewData.confidence : 'N/A'}</span>`;
                });
                
                // Add fade-in animation
                tbody.classList.add('fade-in');
            })
            .catch(error => {
                console.error('Error fetching latest reviews:', error);
                const tbody = document.getElementById('latest-review-body');
                tbody.innerHTML = '<tr><td colspan="5" class="text-center text-danger"><i class="fas fa-exclamation-triangle mr-2"></i>Error loading data</td></tr>';
            });
    }

    function fetchLiveSentimentSummary() {
        fetch('/api/live_sentiment_summary')
            .then(response => response.json())
            .then(data => {
                const ctx = document.getElementById('liveSentimentChart').getContext('2d');
                if (liveSentimentChartInstance) {
                    liveSentimentChartInstance.destroy();
                }
                liveSentimentChartInstance = new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: data.labels,
                        datasets: [{
                            label: 'Sentiment Count',
                            data: data.data,
                            backgroundColor: [
                                'rgba(75, 192, 192, 0.7)', // Positive
                                'rgba(255, 206, 86, 0.7)', // Neutral
                                'rgba(255, 99, 132, 0.7)'  // Negative
                            ],
                            borderColor: [
                                'rgba(75, 192, 192, 1)',
                                'rgba(255, 206, 86, 1)',
                                'rgba(255, 99, 132, 1)'
                            ],
                            borderWidth: 1
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            y: {
                                beginAtZero: true
                            }
                        },
                        plugins: {
                            legend: {
                                display: false
                            }
                        }
                    }
                });
            })
            .catch(error => console.error('Error fetching live sentiment summary:', error));
    }

    // --- Offline Panel Functions ---
    function fetchSentimentOverTime(asin = null) {
        let url = '/api/sentiment_over_time';
        if (asin) {
            url += `?asin=${asin}`;
        }
        // Add date range parameters here if implemented

        fetch(url)
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    console.error("Error fetching sentiment over time:", data.error);
                    document.getElementById('sentimentOverTimeChart').innerHTML = `<p class="text-danger">Error: ${data.error}</p>`;
                    return;
                }
                const ctx = document.getElementById('sentimentOverTimeChart').getContext('2d');
                if (sentimentOverTimeChartInstance) {
                    sentimentOverTimeChartInstance.destroy();
                }
                sentimentOverTimeChartInstance = new Chart(ctx, {
                    type: 'line', // or 'bar'
                    data: {
                        labels: data.labels,
                        datasets: [
                            {
                                label: 'Positive',
                                data: data.datasets.Positive,
                                borderColor: 'rgba(75, 192, 192, 1)',
                                backgroundColor: 'rgba(75, 192, 192, 0.2)',
                                fill: false,
                                tension: 0.1
                            },
                            {
                                label: 'Neutral',
                                data: data.datasets.Neutral,
                                borderColor: 'rgba(255, 206, 86, 1)',
                                backgroundColor: 'rgba(255, 206, 86, 0.2)',
                                fill: false,
                                tension: 0.1
                            },
                            {
                                label: 'Negative',
                                data: data.datasets.Negative,
                                borderColor: 'rgba(255, 99, 132, 1)',
                                backgroundColor: 'rgba(255, 99, 132, 0.2)',
                                fill: false,
                                tension: 0.1
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        scales: {
                            y: {
                                beginAtZero: true
                            }
                        }
                    }
                });
            })
            .catch(error => console.error('Error fetching sentiment over time:', error));
    }
    
    function loadAvailableProducts() {
        fetch('/api/available_products')
            .then(response => response.json())
            .then(data => {
                const productSelect = document.getElementById('productSelect');
                productSelect.innerHTML = '<option value="">-- Select a product --</option>';
                
                if (Array.isArray(data) && data.length > 0) {
                    data.forEach(product => {
                        const option = document.createElement('option');
                        if (typeof product === 'object' && product.asin) {
                            option.value = product.asin;
                            option.textContent = `${product.asin} (${product.review_count} reviews)`;
                        } else {
                            option.value = product;
                            option.textContent = product;
                        }
                        productSelect.appendChild(option);
                    });
                } else {
                    // Fallback to default products
                    const defaultProducts = ["B00004Y2UT", "B00005Y2UX", "B00006Y2UZ"];
                    defaultProducts.forEach(asin => {
                        const option = document.createElement('option');
                        option.value = asin;
                        option.textContent = asin;
                        productSelect.appendChild(option);
                    });
                }
            })
            .catch(error => {
                console.error('Error loading products:', error);
                const productSelect = document.getElementById('productSelect');
                productSelect.innerHTML = '<option value="">Error loading products</option>';
            });
    }

    function fetchProductAnalysis(asin) {
        if (!asin) {
            document.getElementById('productAnalysisContent').style.display = 'none';
            document.getElementById('selectedAsinDisplay').innerHTML = '<i class="fas fa-info-circle mr-1"></i>No product selected';
            document.getElementById('selectedAsinDisplay').className = 'badge badge-secondary';
            return;
        }

        // Show loading state
        document.getElementById('selectedAsinDisplay').innerHTML = '<i class="fas fa-spinner fa-spin mr-1"></i>Analyzing...';
        document.getElementById('selectedAsinDisplay').className = 'badge badge-warning';
        document.getElementById('productAnalysisContent').style.display = 'block';
        
        showLoading('productSummary');
        showLoading('recentReviewsTable');

        selectedAsin = asin;

        fetch(`/api/product_analysis/${asin}`)
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    console.error(`Error fetching product analysis for ${asin}:`, data.error);
                    document.getElementById('productSummary').innerHTML = `<strong><i class="fas fa-exclamation-triangle mr-2"></i>Error:</strong> ${data.error}`;
                    document.getElementById('productSummary').className = 'alert alert-danger';
                    document.getElementById('selectedAsinDisplay').innerHTML = '<i class="fas fa-exclamation-triangle mr-1"></i>Error';
                    document.getElementById('selectedAsinDisplay').className = 'badge badge-danger';
                    return;
                }

                // Update selected product display
                document.getElementById('selectedAsinDisplay').innerHTML = `<i class="fas fa-check mr-1"></i>${asin}`;
                document.getElementById('selectedAsinDisplay').className = 'badge badge-success';

                // Update product summary with better styling
                const summaryHtml = `
                    <div class="row">
                        <div class="col-md-3 text-center">
                            <h4 class="text-primary"><i class="fas fa-box mr-2"></i>${data.asin}</h4>
                            <small class="text-muted">Product ASIN</small>
                        </div>
                        <div class="col-md-3 text-center">
                            <h4 class="text-info">${data.total_reviews}</h4>
                            <small class="text-muted">Total Reviews</small>
                        </div>
                        <div class="col-md-3 text-center">
                            <h4 class="text-success">${data.confidence_stats.average}</h4>
                            <small class="text-muted">Avg Confidence</small>
                        </div>
                        <div class="col-md-3 text-center">
                            <h4 class="text-warning"><i class="fas fa-clock mr-1"></i></h4>
                            <small class="text-muted">${new Date().toLocaleString()}</small>
                        </div>
                    </div>
                `;
                document.getElementById('productSummary').innerHTML = summaryHtml;
                document.getElementById('productSummary').className = 'alert alert-info';

                // Sentiment Distribution Pie Chart
                const pieCtx = document.getElementById('productSentimentPieChart').getContext('2d');
                if (productSentimentPieChartInstance) {
                    productSentimentPieChartInstance.destroy();
                }
                productSentimentPieChartInstance = new Chart(pieCtx, {
                    type: 'pie',
                    data: {
                        labels: data.sentiment_distribution.labels,
                        datasets: [{
                            data: data.sentiment_distribution.data,
                            backgroundColor: [
                                'rgba(75, 192, 192, 0.7)',
                                'rgba(255, 206, 86, 0.7)',
                                'rgba(255, 99, 132, 0.7)'
                            ],
                            borderColor: [
                                'rgba(75, 192, 192, 1)',
                                'rgba(255, 206, 86, 1)',
                                'rgba(255, 99, 132, 1)'
                            ],
                            borderWidth: 1
                        }]
                    },
                    options: { 
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: {
                                position: 'bottom'
                            }
                        }
                    }
                });

                // Review Count Over Time Chart
                const lineCtx = document.getElementById('productReviewCountChart').getContext('2d');
                if (productReviewCountChartInstance) {
                    productReviewCountChartInstance.destroy();
                }
                productReviewCountChartInstance = new Chart(lineCtx, {
                    type: 'line',
                    data: {
                        labels: data.review_count_over_time.labels,
                        datasets: [{
                            label: `Review Count for ${asin}`,
                            data: data.review_count_over_time.data,
                            borderColor: 'rgba(54, 162, 235, 1)',
                            backgroundColor: 'rgba(54, 162, 235, 0.2)',
                            fill: true,
                            tension: 0.1
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: { 
                            y: { beginAtZero: true }
                        }
                    }
                });

                // Enhanced Recent Reviews Table
                const recentReviewsTable = document.getElementById('recentReviewsTable');
                recentReviewsTable.innerHTML = '';

                if (data.recent_reviews && data.recent_reviews.length > 0) {
                    data.recent_reviews.forEach(review => {
                        const row = recentReviewsTable.insertRow();
                        
                        // Date with icon
                        const dateCell = row.insertCell();
                        const date = new Date(review.timestamp).toLocaleDateString();
                        dateCell.innerHTML = `<small><i class="fas fa-calendar-alt mr-1"></i>${date}</small>`;
                        
                        // Sentiment with enhanced badge
                        const sentimentCell = row.insertCell();
                        let badgeClass = 'badge-secondary';
                        let icon = 'fas fa-meh';
                        if (review.sentiment === 'Positive') {
                            badgeClass = 'badge-success';
                            icon = 'fas fa-smile';
                        } else if (review.sentiment === 'Negative') {
                            badgeClass = 'badge-danger';
                            icon = 'fas fa-frown';
                        } else if (review.sentiment === 'Neutral') {
                            badgeClass = 'badge-warning';
                            icon = 'fas fa-meh';
                        }
                        sentimentCell.innerHTML = `<span class="badge ${badgeClass}"><i class="${icon} mr-1"></i>${review.sentiment}</span>`;
                        
                        // Confidence with progress bar
                        const confidenceCell = row.insertCell();
                        const confidence = review.confidence ? (review.confidence * 100).toFixed(0) : 0;
                        confidenceCell.innerHTML = `
                            <div class="progress" style="height: 20px;">
                                <div class="progress-bar bg-info" role="progressbar" style="width: ${confidence}%">
                                    ${confidence}%
                                </div>
                            </div>
                        `;
                        
                        // Review Text with better formatting
                        const textCell = row.insertCell();
                        textCell.innerHTML = `<small class="text-muted">${review.reviewTextShort || 'No text available'}</small>`;
                        
                        // Rating with stars
                        const ratingCell = row.insertCell();
                        const rating = review.overall || 0;
                        let stars = '';
                        for (let i = 1; i <= 5; i++) {
                            if (i <= rating) {
                                stars += '<i class="fas fa-star text-warning"></i>';
                            } else {
                                stars += '<i class="far fa-star text-muted"></i>';
                            }
                        }
                        ratingCell.innerHTML = stars;
                    });
                } else {
                    const row = recentReviewsTable.insertRow();
                    const cell = row.insertCell();
                    cell.colSpan = 5;
                    cell.innerHTML = '<div class="text-center text-muted"><i class="fas fa-inbox mr-2"></i>No recent reviews available</div>';
                }
                
                // Add fade-in animation
                document.getElementById('productAnalysisContent').classList.add('fade-in');
            })
            .catch(error => {
                console.error(`Error fetching product analysis for ${asin}:`, error);
                document.getElementById('productSummary').innerHTML = `<strong><i class="fas fa-exclamation-triangle mr-2"></i>Error:</strong> Failed to load product analysis`;
                document.getElementById('productSummary').className = 'alert alert-danger';
                document.getElementById('selectedAsinDisplay').innerHTML = '<i class="fas fa-exclamation-triangle mr-1"></i>Error';
                document.getElementById('selectedAsinDisplay').className = 'badge badge-danger';
            });
    }

    function fetchGlobalStats() {
        fetch('/api/global_stats')
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    console.error("Error fetching global stats:", data.error);
                    document.getElementById('globalStatsContent').innerHTML = `<p class="text-danger">Error: ${data.error}</p>`;
                    return;
                }
                const contentDiv = document.getElementById('globalStatsContent');
                let html = `<p><strong>Total Reviews Processed:</strong> ${data.total_reviews_processed}</p>`;
                
                html += `<h6>Overall Sentiment Distribution:</h6><ul>`;
                for (const sentiment in data.sentiment_distribution_all) {
                    html += `<li>${sentiment}: ${data.sentiment_distribution_all[sentiment]}</li>`;
                }
                html += `</ul>`;

                html += `<h6>Top 5 Most Reviewed Products:</h6><ol>`;
                data.top_5_most_reviewed_products.forEach(p => {
                    html += `<li>${p._id} (${p.count} reviews)</li>`;
                });
                html += `</ol>`;

                html += `<h6>Top 5 Most Negative Products (by count of negative reviews):</h6><ol>`;
                data.top_5_most_negative_products.forEach(p => {
                    html += `<li>${p._id} (${p.count} negative reviews)</li>`;
                });
                html += `</ol>`;
                
                contentDiv.innerHTML = html;
            })
            .catch(error => console.error('Error fetching global stats:', error));
    }

    // Event listeners
    document.getElementById('applySotFilter').addEventListener('click', () => {
        const asin = document.getElementById('filterAsinSOT').value.trim();
        fetchSentimentOverTime(asin || null);
    });

    document.getElementById('analyzeProduct').addEventListener('click', () => {
        const asin = document.getElementById('productSelect').value;
        if (asin) {
            fetchProductAnalysis(asin);
        } else {
            alert('Please select a product first');
        }
    });

    // Initial data load
    fetchLatestReview();
    fetchLiveSentimentSummary();
    fetchSentimentOverTime();
    loadAvailableProducts();
    fetchGlobalStats();

    // Auto-refresh for online panel (optional)
    setInterval(fetchLatestReview, 5000);
    setInterval(fetchLiveSentimentSummary, 30000);
});
