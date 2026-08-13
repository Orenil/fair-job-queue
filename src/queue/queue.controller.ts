import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { QueueService } from './queue.service';
import { EnqueueOptions } from './types';

@Controller('queue')
export class QueueController {
  constructor(private readonly queue: QueueService) {}

  @Post('enqueue')
  async enqueue(@Body() body: EnqueueOptions): Promise<{ id: string }> {
    const id = await this.queue.enqueue(body);
    return { id };
  }

  @Post('tenants/:tenantId/weight')
  async setWeight(
    @Param('tenantId') tenantId: string,
    @Body('weight') weight: number,
  ): Promise<{ ok: true }> {
    await this.queue.setTenantWeight(tenantId, weight);
    return { ok: true };
  }

  @Get('jobs/:id')
  async getJob(@Param('id') id: string) {
    return this.queue.getJob(id);
  }

  @Get('stats')
  async stats() {
    return this.queue.stats();
  }
}
