using RabbitMQ.Client;
using lab1.Services; // Đã có, rất tốt

namespace lab1
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            // 1. Đăng ký ConnectionFactory (Để gửi tin nhắn)
            builder.Services.AddSingleton(sp => new ConnectionFactory
            {
                HostName = "localhost"
            });

            // 2. ĐĂNG KÝ WORKER (Để nhận tin nhắn) - Dòng này bạn đang thiếu
            // Nếu không có dòng này, RabbitWorker sẽ không bao giờ được khởi chạy
            builder.Services.AddHostedService<RabbitWorker>();

            builder.Services.AddControllers();
            builder.Services.AddEndpointsApiExplorer();
            builder.Services.AddSwaggerGen();

            var app = builder.Build();

            if (app.Environment.IsDevelopment())
            {
                app.UseSwagger();
                app.UseSwaggerUI();
            }

            app.UseHttpsRedirection();
            app.UseAuthorization();
            app.MapControllers();

            app.Run();
        }
    }
}