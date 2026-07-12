import { Module } from '@nestjs/common';
import { HealthController } from './health.controller';
import { TcpGatewayService } from './tcp-gateway.service';

@Module({
  controllers: [HealthController],
  providers: [TcpGatewayService],
})
export class AppModule {}
