# Shopping Sessions: C++ pipeline. Builds a static linux/amd64 binary, runs it in a slim image.
FROM gcc:13-bookworm AS build
WORKDIR /src
COPY src/ src/
RUN g++ -O3 -std=c++17 -pthread -static -o /pipeline src/main.cpp

FROM debian:bookworm-slim
COPY --from=build /pipeline /app/pipeline
WORKDIR /app
ENV GLIBC_TUNABLES=glibc.malloc.hugetlb=1
CMD ["/app/pipeline"]
