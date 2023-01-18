@echo off
Echo **Nuget Pack**

set suffix=
Echo **version%suffix%**

cd ../src/

rm -rf bin/*
dotnet pack ./DotNet.Utils/DotNet.Utils.csproj --version-suffix "%suffix%" -o bin -c Release

cd bin

nuget push *.nupkg -Source https://api.nuget.org/v3/index.json


pause
