# One Dockerfile for every service; SERVICE selects which cmd/ to build.
FROM golang:1.27-alpine AS build
WORKDIR /src

# Dependencies first, so a source-only change does not re-download modules.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG SERVICE
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/service ./cmd/${SERVICE}

# Distroless-style final image: no shell, no package manager, nothing to attack.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/service /service
USER nonroot:nonroot
ENTRYPOINT ["/service"]
