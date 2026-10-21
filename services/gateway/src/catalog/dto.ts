import { Type } from 'class-transformer';
import {
  ArrayMaxSize,
  ArrayNotEmpty,
  IsArray,
  IsInt,
  IsISO8601,
  IsNotEmpty,
  IsOptional,
  IsString,
  IsUUID,
  Length,
  Max,
  MaxLength,
  Min,
  ValidateNested,
} from 'class-validator';

// Chỉ kiểm tra kiểu và chặn payload quá lớn; quy tắc nghiệp vụ (tên tier trùng, starts_at ở quá khứ,
// giới hạn số ghế mỗi event...) do catalog kiểm tra và trả INVALID_ARGUMENT => 400.

export class CreateVenueDto {
  @IsString()
  @IsNotEmpty()
  @MaxLength(200)
  name: string;

  @IsOptional()
  @IsString()
  @MaxLength(500)
  address?: string;
}

export class TierDto {
  @IsString()
  @IsNotEmpty()
  @MaxLength(50)
  name: string;

  /** đơn vị nhỏ nhất (VND: đồng) */
  @IsInt()
  @Min(0)
  @Max(1_000_000_000_000)
  priceAmount: number;

  @IsOptional()
  @IsString()
  @Length(3, 3)
  currency?: string;
}

export class CreateEventDto {
  @IsString()
  @IsNotEmpty()
  @MaxLength(200)
  name: string;

  @IsOptional()
  @IsString()
  @MaxLength(5000)
  description?: string;

  @IsUUID()
  venueId: string;

  @IsISO8601({ strict: true })
  startsAt: string;

  @IsArray()
  @ArrayNotEmpty()
  @ArrayMaxSize(20)
  @ValidateNested({ each: true })
  @Type(() => TierDto)
  tiers: TierDto[];
}

export class SeatBlockDto {
  @IsString()
  @IsNotEmpty()
  @MaxLength(20)
  section: string;

  @IsInt()
  @Min(1)
  @Max(500)
  rowCount: number;

  @IsInt()
  @Min(1)
  @Max(500)
  seatsPerRow: number;

  @IsUUID()
  tierId: string;
}

export class GenerateSeatsDto {
  @IsArray()
  @ArrayNotEmpty()
  @ArrayMaxSize(50)
  @ValidateNested({ each: true })
  @Type(() => SeatBlockDto)
  blocks: SeatBlockDto[];
}

export class ListEventsQuery {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(100)
  pageSize?: number;

  @IsOptional()
  @IsString()
  @MaxLength(500)
  pageToken?: string;
}
