FROM node:18-slim
WORKDIR /app
COPY package*.json ./
RUN npm ci --production
LABEL yarddesk.static-revision="2026-08-15.1"
COPY . .
EXPOSE 3000
CMD ["npm", "start"]
