package main

// NewsRequest represents the incoming news request
type NewsRequest struct {
	Headline  string `json:"headline"`
	URL       string `json:"url"`
	Published string `json:"published"`
	FullText  string `json:"full_text,omitempty"`
}

// NewsResponse represents the response from the news endpoint
type NewsResponse struct {
	NewsID string `json:"news_id"`
	Status string `json:"status"`
}

// KafkaMessage represents a message to be sent to Kafka
type KafkaMessage struct {
	NewsID    string `json:"news_id"`
	Headline  string `json:"headline"`
	URL       string `json:"url"`
	Published string `json:"published"`
	FullText  string `json:"full_text,omitempty"`
}