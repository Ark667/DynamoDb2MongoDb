#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["DynamoDb2MongoDb/DynamoDb2MongoDb.csproj", "DynamoDb2MongoDb/"]
RUN dotnet restore "DynamoDb2MongoDb/DynamoDb2MongoDb.csproj"
COPY . .
WORKDIR "/src/DynamoDb2MongoDb"
RUN dotnet build "DynamoDb2MongoDb.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "DynamoDb2MongoDb.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "DynamoDb2MongoDb.dll"]