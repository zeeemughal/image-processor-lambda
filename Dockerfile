# Lambda container image (built for linux/amd64 — Lambda only supports x86_64/arm64):
#   docker buildx build --platform linux/amd64 -t image-processor-lambda .
#   aws ecr get-login-password --region <region> | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
#   docker tag image-processor-lambda:latest <account>.dkr.ecr.<region>.amazonaws.com/image-processor-lambda:latest
#   docker push <account>.dkr.ecr.<region>.amazonaws.com/image-processor-lambda:latest

FROM public.ecr.aws/lambda/nodejs:20

COPY package*.json ./
RUN npm ci --omit=dev

COPY src ./src

CMD ["src/handler.handler"]
