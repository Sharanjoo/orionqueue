# OrionQueue dashboard (React + TypeScript, built with Vite, served by nginx).
FROM node:24-alpine AS build
WORKDIR /src
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM nginx:1.27-alpine AS run
COPY --from=build /src/dist /usr/share/nginx/html
EXPOSE 80
