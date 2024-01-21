import { Prop, Schema, SchemaFactory } from "@nestjs/mongoose";
import { HydratedDocument } from "mongoose";

export type DataDocument = HydratedDocument<sentiment_result>;

@Schema()
export class sentiment_result {
    @Prop({ required: true })
    text: string;

    @Prop({ required: true })
    negative_percentage: number;

    @Prop({ required: true })
    positive_percentage: number;

    @Prop({ required: true })
    neutral_percentage: number;

    @Prop({ required: true })
    compound: number;

    @Prop({ required: true })
    sentiment: string;
}

export const DataSchema = SchemaFactory.createForClass(sentiment_result);