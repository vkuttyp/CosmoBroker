# Build stage
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy project files first for layer-cached restore
COPY CosmoBroker.Server/CosmoBroker.Server.csproj CosmoBroker.Server/
COPY CosmoBroker.Management/CosmoBroker.Management.csproj CosmoBroker.Management/
COPY CosmoBroker.csproj ./

RUN dotnet restore CosmoBroker.Server/CosmoBroker.Server.csproj
RUN dotnet restore CosmoBroker.Management/CosmoBroker.Management.csproj

# Copy full source and publish both executables
COPY . .
RUN dotnet publish CosmoBroker.Server/CosmoBroker.Server.csproj -c Release -o /app/server
RUN dotnet publish CosmoBroker.Management/CosmoBroker.Management.csproj -c Release -o /app/management

# Runtime stage
FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app

RUN useradd --no-create-home --shell /bin/false cosmobroker

COPY --from=build /app/server /app/server
COPY --from=build /app/management /app/management
COPY docker/combined/entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh
USER cosmobroker

EXPOSE 4222
EXPOSE 8222
EXPOSE 5672
EXPOSE 9091

ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=true
ENV COSMOBROKER_ENABLE_NATS=true
ENV COSMOBROKER_ENABLE_AMQP=false
ENV COSMOBROKER_AMQP_PORT=5672
ENV COSMOBROKER_MANAGEMENT_PORT=9091
ENV COSMOBROKER_MONITOR_URL=http://127.0.0.1:8222

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
  CMD sh -c 'echo "" | nc -w 1 localhost 8222 && echo "" | nc -w 1 localhost 9091 && exit 0 || exit 1'

ENTRYPOINT ["/app/entrypoint.sh"]
