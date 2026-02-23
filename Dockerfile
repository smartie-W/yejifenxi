FROM node:22-alpine

WORKDIR /app

COPY package.json ./
COPY server.mjs ./
COPY index.html ./
COPY app.js ./
COPY styles.css ./
COPY selftest.html ./
COPY selftest.js ./
COPY assets ./assets
COPY data ./data
COPY scripts ./scripts

ENV NODE_ENV=production
ENV PORT=8080
ENV HOST=0.0.0.0

EXPOSE 8080

CMD ["node", "server.mjs"]
