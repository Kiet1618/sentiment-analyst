import { Injectable } from '@nestjs/common';
import { sentiment_result, DataDocument } from "./data.schema";
import { InjectModel } from "@nestjs/mongoose";
import { Model } from "mongoose";

@Injectable()
export class AppService {
  constructor(
    @InjectModel(sentiment_result.name)
    private dataModel: Model<DataDocument>,
  ) { }

  async getData(): Promise<sentiment_result[]> {
    return this.dataModel.find().exec();
  }

}
