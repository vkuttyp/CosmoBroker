# Build stage
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy project files first for layer-cached restore
COPY CosmoBroker.Management/CosmoBroker.Management.csproj CosmoBroker.Management/

RUN dotnet restore CosmoBroker.Management/CosmoBroker.Management.csproj

# Copy full source and publish
COPY . .
RUN dotnet publish CosmoBroker.Management/CosmoBroker.Management.csproj -c Release -o /app/publish

# Runtime stage
FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app

RUN useradd --no-create-home --shell /bin/false cosmobroker
USER cosmobroker

COPY --from=build /app/publish .

EXPOSE 9091

ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=true
ENV COSMOBROKER_MANAGEMENT_PORT=9091
ENV COSMOBROKER_MONITOR_URL=http://cosmobroker:8222

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD sh -c 'echo "" | nc -w 1 localhost 9091 && exit 0 || exit 1'

ENTRYPOINT ["dotnet", "CosmoBroker.Management.dll"]
