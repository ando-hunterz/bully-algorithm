FROM node:16-alpine

WORKDIR /usr/src/app
# Install Node js and npm
COPY package*.json ./

RUN npm ci --only=production

COPY . .

CMD ["node","bully.js"]