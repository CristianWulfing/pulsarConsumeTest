using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using producerTest;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
