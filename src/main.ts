import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  const port = Number(process.env.PORT ?? 3100);
  await app.listen(port);
  // eslint-disable-next-line no-console
  console.log(`fair-job-queue API listening on :${port}`);
}

bootstrap();
