package main

import (
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
)

func main() {
	// Create a new Gin router
	router := gin.Default()

	// Use middleware to log the request body for all routes
	router.Use(logRequestBodyMiddleware())

	// Handle all routes with a wildcard route
	router.Any("/*any", func(c *gin.Context) {
		// Respond with application/json {"status": "ok"}
		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
		})
	})

	// Start the server on port 8080
	router.Run(":8080")
}

// Middleware to log request body
func logRequestBodyMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Read the request body
		bodyBytes, err := io.ReadAll(c.Request.Body)
		if err != nil {
			fmt.Println("Error reading body:", err)
		}

		// Log the body
		fmt.Println("Request Body:", string(bodyBytes))

		// Process the request
		c.Next()
	}
}
