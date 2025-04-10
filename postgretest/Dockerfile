FROM mcr.microsoft.com/dotnet/runtime:9.0 AS base
USER root
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["postgretest/postgretest.csproj", "postgretest/"]
RUN dotnet restore "postgretest/postgretest.csproj"
COPY . .
WORKDIR "/src/postgretest"
RUN dotnet build "postgretest.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "postgretest.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
COPY ["postgretest/ips.txt","/RaftProject/ips.txt"]
RUN mkdir -p /RaftProject
RUN chmod 777 -R /RaftProject
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "postgretest.dll"]
