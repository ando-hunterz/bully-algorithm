FROM alpine:latest

WORKDIR /usr/src/app
# Install Node js and npm
RUN apk add --update nodejs npm

COPY . .

RUN npm install



CMD ["node","bully.js"]