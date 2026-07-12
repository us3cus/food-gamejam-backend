import { Controller, Get } from '@nestjs/common';
import { TcpGatewayService } from './tcp-gateway.service';

@Controller()
export class HealthController {
  constructor(private readonly gateway: TcpGatewayService) {}

  @Get('health')
  health() {
    return { status: 'ok', tcp_port: this.gateway.port, lobbies: this.gateway.lobbyCount };
  }
}
