# Build stage
FROM golang:1.23-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

WORKDIR /app

# Install templ CLI for template generation
RUN go install github.com/a-h/templ/cmd/templ@v0.3.960

# Install air for hot reloading (pinned for Go 1.23 compatibility)
RUN go install github.com/air-verse/air@v1.61.7

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Generate templates
RUN templ generate

# Build the application
RUN go build -o usds .

# Final stage - use golang image for hot reloading support
FROM golang:1.23-alpine

RUN apk --no-cache add ca-certificates git

WORKDIR /app

# Copy binaries
COPY --from=builder /app/usds .
COPY --from=builder /go/bin/air /usr/local/bin/air
COPY --from=builder /go/bin/templ /usr/local/bin/templ

# Copy source for hot reloading
COPY --from=builder /app/ .

EXPOSE 8080

CMD ["air", "-c", ".air.toml"]
