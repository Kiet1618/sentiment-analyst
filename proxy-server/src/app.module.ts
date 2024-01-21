import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { MongooseModule } from "@nestjs/mongoose";


import {
  sentiment_result,
  DataSchema,
} from "./data.schema";
@Module({
  imports: [
    MongooseModule.forRootAsync({
      imports: [],
      useFactory: async () => {
        return {
          uri: "mongodb+srv://bigdatapyspark:bigdatapyspark@bigdata.d648sgf.mongodb.net/youtube_sentiment?retryWrites=true&w=majority"
        };
      },
    }),
    MongooseModule.forFeature([
      { name: sentiment_result.name, schema: DataSchema },
    ]),
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule { }
