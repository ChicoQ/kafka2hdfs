package nz.co.airnz.bigdata.services.batching;

import nz.co.airnz.bigdata.acars.pojo.DDSDataRecord;
import nz.co.airnz.bigdata.acars.pojo.DDSHeaderRecord;
import nz.co.airnz.bigdata.acars.pojo.DDSRecordMetaData;
import nz.co.airnz.bigdata.acars.pojo.DDSTrailerRecord;
import nz.co.airnz.bigdata.acars.service.AppConfigService;
import nz.co.airnz.bigdata.acars.util.DateUtils;
import nz.co.airnz.bigdata.acars.util.SparkUtils;
import nz.co.airnz.bigdata.acars.util.StringUtils;
import nz.co.airnz.bigdata.acars.util.UUIDGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Processing DDS data in .txt file format to parquet in raw zone
 *
 * 1. A header file to provide detail information of the output file.
 * File header example:
 *  12011100920111009ARC 010PROD2017022318372C3102UST01
 *
 * 2. Rows of DDS records example:
 * 220111009201110999  1210003258701351446               17329406            XR109420111006                               6                              002311.1082 00000000000002493.52950000000000000000000000000000000000000000000000000000000X    -                                                                 11279005904008226141       KR011 1PVGMELO201110100715P201110110905ACA 177  C BAGT   00000000000                  000000000000000000000000000000000N3308  24 231CA00162SUN KWANG WORLD TRAVEL INC.                       SHAMEL      0498800000000000PVGMEL00000000000002311.1082C SEOUL                    03182     177  PVGCA C 308027
 Z01468730
 *
 * 3. A trailer record to indicate the end of the output file with a record count check
 * File trailer example:
 * Z01468730
 */
public class DDSLandingToRawBatching {
    private static Logger logger = LoggerFactory.getLogger(DDSLandingToRawBatching.class);
    public static SaveMode parquetSaveMode = SaveMode.Append;

    public static void main(String[] args) {
        logger.debug("Starting process data ...");

        if(args.length != 1) {
            throw new RuntimeException("Incorrect parameters !");
        }

        String hdfsDir= args[0]; // Input format: "/data/landing/dds"
        if(StringUtils.isEmpty(hdfsDir)) {
            throw new RuntimeException("Incorrect directory path !");
        }

        JavaSparkContext sc = SparkUtils.getSparkContext(AppConfigService.getConfig("dds.spark.app.name.landing2raw"));

        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path(hdfsDir));
            for (int i = 0; i < status.length; i++) {
                String fileName = hdfsDir + "/" + status[i].getPath().getName(); // dir
                fileName += "/" + status[i].getPath().getName(); // actual input file

                System.out.println("File name: " + fileName);

                processDDSLandingData(fileName, sc);
            }
        } catch (Exception e) {
            System.out.println("File not found !" + e.getMessage());
            e.printStackTrace();
        }

        sc.stop();
    }

    // DDS header sample: 12011101620111016ARC 010PROD2017022321312C3102UST01
    private static void processDDSLandingData(String rawTextFile, JavaSparkContext sc) {
        if(rawTextFile == null) {
            throw new RuntimeException("Invalid raw text file !");
        }

        SQLContext sqlContext = new SQLContext(sc);

        String globalUID = UUIDGenerator.nextUUID();

        JavaRDD<String> fileContent = sc.textFile(rawTextFile);

        // 1. Processing header
        String headerLine = fileContent.first();

        if(StringUtils.isNotBlank(headerLine)) {
            // headerLine : "12011101620111016ARC 010PROD2017022321312C3102UST01   "
            System.out.println("headerLine : " + headerLine);

            DDSHeaderRecord record = new DDSHeaderRecord();

            // Metadata
            record.setGlobalUID(globalUID);

            // Raw data
            record.setSystemProviderReportingPeriodStartDate(Integer.valueOf(headerLine.substring(1,9)));
            record.setSystemProviderReportingPeriodEndingDate(Integer.valueOf(headerLine.substring(9,17)));
            record.setReportingSystemId(headerLine.substring(17,21));
            record.setRevisionNumber(Integer.valueOf(headerLine.substring(21,24)));
            record.setTestProductionStatus(headerLine.substring(24,28));
            record.setProcessingDate(Integer.valueOf(headerLine.substring(28,36)));
            record.setProcessingTime(Integer.valueOf(headerLine.substring(36, 40)));
            record.setProductionIdVersion(headerLine.substring(40, 46));
            record.setIsoCountryCode(headerLine.substring(46, 48));
            record.setFileType(headerLine.substring(48, 49));
            record.setFileTypeSequenceNumber(headerLine.substring(49, 51));
            record.setFiller(headerLine.substring(51));

            //System.out.println(record);

            List<DDSHeaderRecord> headerRecordList = new ArrayList<DDSHeaderRecord>();
            headerRecordList.add(record);

            sqlContext.createDataFrame(headerRecordList, DDSHeaderRecord.class)
                    .select(
                            col("recordId").as("RECORD_IDENTIFIER"),  //TODO ???
                            col("systemProviderReportingPeriodStartDate").as("SYSTEM_PROVIDER_RPTG_PERIOD_START_DATE"),
                            col("systemProviderReportingPeriodEndingDate").as("SYSTEM_PROVIDER_RPTG_PERIOD_ENDING_DATE"),
                            col("reportingSystemId").as("REPORTING_SYSTEM_IDENTIFIER"),
                            col("revisionNumber").as("REVISION_NUMBER"),
                            col("testProductionStatus").as("TEST_PRODUCTION_STATUS"),
                            col("processingDate").as("PROCESSING_DATE"),
                            col("processingTime").as("PROCESSING_TIME"),
                            col("productionIdVersion").as("PRODUCT_ID_VERSION"),
                            col("isoCountryCode").as("ISO_COUNTRY_CODE"),
                            col("fileType").as("FILE_TYPE"),
                            col("fileTypeSequenceNumber").as("FILE_TYPE_SEQUENCE_NUMBER"),
                            col("filler").as("FILLER")
                    )
                    //.coalesce(3)
                    .write()
                    .mode(parquetSaveMode)
                    .parquet(AppConfigService.getConfig("dds.parquet.dir.raw") + "/dds_header.parquet");
        }

        // 2. Processing EDS raw data
        // Sample record: 220111002201110001  1127100556700000024               93500632            XI001120110926                               6                              000130.1161 00000000000000163.68670000000000000000000000000000000000000000000000000000000R    +                                                                                            TT014 1POSMIAX201110120650A201110121045AAA 1818 Q BAGT   00000000000                  000000000000000000000000000000000Y75719 2  166AA00162V'S TRAVEL CENTRE                                 POSMIA      0162600000000000POSLAX0000000000000000000000DYCHAGUANAS                          1818 POSAA S 302616
        JavaRDD<DDSDataRecord> rdd = fileContent.filter(line -> {
            boolean isRawData = false;
            if (StringUtils.isNotEmpty(line) && line.charAt(0) == '2') {
                isRawData = true;
            }

            return isRawData;
        }).map(row -> {
            DDSDataRecord result = new DDSDataRecord();

            result.setGlobalUID(globalUID);

            result.setProcessingPeriodEndDate(DateUtils.getDateAsString(row.substring(1,9)));
            result.setMonth445(StringUtils.getNumbericValueAsInteger(row.substring(9,15)));
            result.setTicketingCarrierValidatingSuppCode(row.substring(15,20));
            result.setTicketId(row.substring(20,39));
            result.setPrimaryTicketDocNumber(row.substring(39,54));
            result.setAgencyCodeNumber(row.substring(54,62));
            result.setBookingAgencyLocationNumber(row.substring(62,72));
            result.setStatisticalCode(row.substring(72,75));
            result.setTranxCode(row.substring(75,76));
            result.setSystemProviderId(row.substring(76,80));

            result.setDateOfIssue(DateUtils.getDateAsString(row.substring(80,88)));

            result.setTicketDocNumber(row.substring(88,103));

            // BDIC-109: Separate AIRLINE SYSTEM DATA and  PNR_REFERENCE
            String temp = row.substring(103,116);
            if(StringUtils.isNotBlank(temp)) {
                String[] array = temp.split("/");

                if (array.length == 2) {
                    result.setPnrReference(StringUtils.isNotBlank(array[0]) ? array[0] : "" );
                    result.setAirlineSystemData(StringUtils.isNotBlank(array[1]) ? array[1] : "");
                } else {
                    result.setPnrReference(StringUtils.isNotBlank(temp) ? temp : "");
                }
            }

            result.setConjunctionCompanionTicketIndicator(row.substring(116,119));
            result.setElectronicTickeInd(row.substring(119,120));
            result.setFareBasicTicketDeginator(row.substring(120, 135));
            result.setTourCode(row.substring(135,150));
            result.setFareAmount(StringUtils.getNumbericValueAsDouble(row.substring(150,161)));
            result.setFareCalculationModeIndicator(row.substring(161,162));
            result.setNetFareAmount(StringUtils.getNumbericValueAsDouble(row.substring(162,173)));
            result.setTicketDocAmount(StringUtils.getNumbericValueAsDouble(row.substring(173,184)));
            result.setCommissionAmount(StringUtils.getNumbericValueAsDouble(row.substring(184, 195)));
            result.setSupplementaryAmount(StringUtils.getNumbericValueAsDouble(row.substring(195, 206)));
            result.setTaxAmount(StringUtils.getNumbericValueAsDouble(row.substring(206, 239)));
            result.setFareEstimationFlag(row.substring(239, 240));
            result.setCurrencyType(row.substring(240, 244));
            result.setSignOfTheValueAmounts(row.substring(244, 246));
            result.setOriginIssueInfoTkt1(row.substring(246, 278));
            result.setOriginIssueInfoTkt2(row.substring(278,310));
            result.setOriginTicketId(row.substring(310, 329));
            result.setExchangRefundCouponNumTkt1(row.substring(329, 333));
            result.setExchangRefundCouponNumTkt2(row.substring(333, 337));
            result.setIsoCountryCode(row.substring(337, 339));
            result.setSegmentId(row.substring(339, 341));
            result.setTranxCouponTotal(row.substring(341, 343));
            result.setCouponNumber(StringUtils.getNumbericValueAsInteger(row.substring(343, 344)));
            result.setOriginAirportCityCode(row.substring(344, 347));
            result.setDestinationAirportCityCode(row.substring(347, 350));
            result.setStopOverCode(row.substring(350, 351));
            result.setFlightDate(DateUtils.getDateAsString(row.substring(351, 359)));

            String[] array = StringUtils.getCustomTimeFormat(row.substring(359, 364));
            result.setFlightDepartureTime(array[0]);
            result.setFlightDepartureTimeAMPM(array[1]);

            result.setFlightArrivalDate(DateUtils.getDateAsString(row.substring(364, 372)));

            array = StringUtils.getCustomTimeFormat(row.substring(372, 377));
            result.setFlightArrivalTime(array[0]);
            result.setFlightArrivalTimeAMPM(array[1]);

            result.setCarrierCode(row.substring(377, 380));
            result.setFlightNumber(StringUtils.getFixed4Chars(row.substring(380, 385)));
            result.setReservationBookingDesignator(row.substring(385, 387));
            result.setDistributionChannel(row.substring(387, 391));
            result.setOriginCurrencyCode(row.substring(391, 394));
            result.setOriginalCurrencyFareAmount(StringUtils.getNumbericValueAsDouble(row.substring(394, 405)));
            result.setExchangeRateApplied(StringUtils.getNumbericValueAsDouble(row.substring(405, 415)));
            result.setDateOfExchangeRateApplication(DateUtils.getDateAsString(row.substring(415, 423)));
            result.setOriginTaxAmounts(row.substring(423, 456));
            result.setContributedDataFlag(row.substring(456, 457));
            result.setAircraftType(row.substring(457, 460));
            result.setSeatsInFirstClass(StringUtils.getNumbericValueAsInteger(row.substring(460, 463)));
            result.setSeatsInBusinessClass(StringUtils.getNumbericValueAsInteger(row.substring(463, 466)));
            result.setSeatsInEconomyClass(StringUtils.getNumbericValueAsInteger(row.substring(466, 469)));
            result.setOperatingCarrier(row.substring(469, 471));
            result.setCounter(StringUtils.getNumbericValueAsInteger(row.substring(471, 476)));

            result.setPointOfSaleTradeName(row.substring(476, 526));
            result.setCityOfAirportLocation(row.substring(526, 532));
            result.setCoTerminalCity(row.substring(532, 538));
            result.setMileageBetweenCityPairs(row.substring(538, 543));
            result.setTotalBaseFareAmount(StringUtils.getNumbericValueAsDouble(row.substring(543, 554)));
            result.setTrueOD(row.substring(554, 560));
            result.setTrueODValue(row.substring(560, 571));
            result.setGlobalIndustryAverageFare(StringUtils.getNumbericValueAsDouble(row.substring(571, 582)));
            result.setDominantCabinClass(row.substring(582, 584));
            result.setAgencyCity(row.substring(584, 609));
            result.setAgencyZipCode(row.substring(609, 619));
            result.setOperatingCarrierFlightNumber(StringUtils.getFixed4Chars(row.substring(619, 624)));
            result.setPointOfOriginAirportCityCode(row.substring(624, 627));
            result.setDominantMarketingCarrier(row.substring(627, 630));
            result.setDominantReservationBookingDesignator(row.substring(630, 632));
            result.setSourceCode(StringUtils.getNumbericValueAsInteger(row.substring(632, 633)));
            result.setKilometresBetweenAirportPairs(row.substring(633, 638));

            //System.out.println(result);

            return result;
        });

        sqlContext.createDataFrame(rdd, DDSDataRecord.class)
                //.show();
                .select(
                    col("globalUID").as("GUID"),
                    col("recordId").as("RECORD_IDENTIFIER"),
                    col("processingPeriodEndDate").as("PROCESSING_PERIOD_ENDING_DATE"),
                    col("month445").as("445_MONTH"),
                    col("ticketingCarrierValidatingSuppCode").as("TICKETING_CARRIER_VALIDATING_SUPP_CODE"),
                    col("ticketId").as("TICKET_ID"),
                    col("primaryTicketDocNumber").as("PRIMARY_TKT_DOCUMENT_NUMBER"),
                    col("agencyCodeNumber").as("AGENCY_CODE_NUMBER"),
                    col("bookingAgencyLocationNumber").as("BOOKING_AGENCY_LOCATION_NUMBER"),
                    col("statisticalCode").as("STATISTICAL_CODE"),
                    col("tranxCode").as("TRANSACTION_CODE"),
                    col("systemProviderId").as("SYSTEM_PROVIDER_IDENTIFIER"),
                    col("dateOfIssue").as("DATE_OF_ISSUE"),
                    col("ticketDocNumber").as("TICKET_DOCUMENT_NUMBER"),

                    col("pnrReference").as("PNR_REFERENCE"),
                    col("airlineSystemData").as("AIRLINE_SYSTEM_DATA"),

                    col("conjunctionCompanionTicketIndicator").as("CONJUNCTION_COMPANION_TICKET_INDICATOR"),
                    col("electronicTickeInd").as("ELECTRONIC_TICKET_IND"),
                    col("fareBasicTicketDeginator").as("FARE_BASIS_TICKET_DESIGNATOR"),
                    col("tourCode").as("TOUR_CODE"),
                    col("fareAmount").as("FARE_AMOUNT"),
                    col("fareCalculationModeIndicator").as("FARE_CALCULATION_MODE_INDICATOR"),
                    col("netFareAmount").as("NET_FARE_AMOUNT"),
                    col("ticketDocAmount").as("TICKET_DOCUMENT_AMOUNT"),
                    col("commissionAmount").as("COMMISSION_AMOUNT"),
                    col("supplementaryAmount").as("SUPPLEMENTARY_AMOUNT"),
                    col("taxAmount").as("TAX_AMOUNT"),
                    col("fareEstimationFlag").as("FARE_ESTIMATION_FLAG"),
                    col("currencyType").as("CURRENCY_TYPE"),
                    col("signOfTheValueAmounts").as("SIGN_OF_THE_VALUE_AMOUNTS"),
                    col("originIssueInfoTkt1").as("ORIG_ISSUE_INFO_TKT1"),
                    col("originIssueInfoTkt2").as("ORIG_ISSUE_INFO_TKT2"),
                    col("originTicketId").as("ORIGINAL_TICKET_ID"),
                    col("exchangRefundCouponNumTkt1").as("EXCHANGED_REFUNDED_CPN_NO_TKT1"),
                    col("exchangRefundCouponNumTkt2").as("EXCHANGED_REFUNDED_CPN_NO_TKT2"),
                    col("isoCountryCode").as("ISO_COUNTRY_CODE"),
                    col("segmentId").as("SEGMENT_IDENTIFIER"),
                    col("tranxCouponTotal").as("TRANSACTION_COUPON_TOTAL"),
                    col("couponNumber").as("COUPON_NUMBER"),
                    col("originAirportCityCode").as("ORIGIN_AIRPORT_CITY_CODE"),
                    col("destinationAirportCityCode").as("DESTINATION_AIRPORT_CITY_CODE"),
                    col("stopOverCode").as("STOPOVER_CODE"),
                    col("flightDate").as("FLIGHT_DATE"),

                    col("flightDepartureTime").as("FLIGHT_DEPARTURE_TIME"),
                    col("flightDepartureTimeAMPM").as("DEPARTURE_AM_PM"),

                    col("flightArrivalDate").as("FLIGHT_ARRIVAL_DATE"),

                    col("flightArrivalTime").as("FLIGHT_ARRIVAL_TIME"),
                    col("flightArrivalTimeAMPM").as("ARRIVAL_AM_PM"),

                    col("carrierCode").as("CARRIER_CODE"),
                    col("flightNumber").as("FLIGHT_NUMBER"),
                    col("reservationBookingDesignator").as("RESERVATION_BOOKING_DESIGNATOR"),
                    col("distributionChannel").as("DISTRIBUTION_CHANNEL"),
                    col("originCurrencyCode").as("ORIGIN_CURRENCY_CODE"),
                    col("originalCurrencyFareAmount").as("ORIGINAL_CURRENCY_FARE_AMOUNT"),
                    col("exchangeRateApplied").as("EXCHANGE_RATE_APPLIED"),
                    col("dateOfExchangeRateApplication").as("DATE_OF_EXCHANGE_RATE_APPLICATION"),
                    col("originTaxAmounts").as("ORIGIN_TAX_AMOUNTS"),
                    col("contributedDataFlag").as("CONTRIBUTED_DATA_FLAG"),
                    col("aircraftType").as("AIRCRAFT_TYPE"),
                    col("seatsInFirstClass").as("SEATS_IN_FIRST_CLASS"),
                    col("seatsInEconomyClass").as("SEATS_IN_ECONOMY_CLASS"),
                    col("seatsInBusinessClass").as("SEATS_IN_BUSINESS_CLASS"),
                    col("operatingCarrier").as("OPERATING_CARRIER"),
                    col("counter").as("COUNTER"),
                    col("pointOfSaleTradeName").as("POINT_OF_SALE_TRADE_NAME"),
                    col("cityOfAirportLocation").as("CITY_OF_AIRPORT_LOCATION"),
                    col("coTerminalCity").as("CO_TERMINAL_CITY"),
                    col("mileageBetweenCityPairs").as("MILEAGE_BETWEEN_CITY_PAIRS"),
                    col("totalBaseFareAmount").as("TOTAL_BASE_FARE_AMOUNT"),
                    col("trueOD").as("TRUE_OD"),
                    col("trueODValue").as("TRUE_OD_VALUE"),
                    col("globalIndustryAverageFare").as("GLOBAL_INDUSTRY_AVERAGE_FARE"),
                    col("dominantCabinClass").as("DOMINANT_CABIN_CLASS"),
                    col("agencyCity").as("AGENCY_CITY"),
                    col("agencyZipCode").as("AGENCY_ZIP_CODE"),
                    col("operatingCarrierFlightNumber").as("OPERATING_CARRIER_FLIGHT_NUMBER"),
                    col("pointOfOriginAirportCityCode").as("POINT_OF_ORIGIN_AIRPORT_CITY_CODE"),
                    col("dominantMarketingCarrier").as("DOMINANT_MARKETING_CARRIER"),
                    col("dominantReservationBookingDesignator").as("DOMINANT_RESERVATION_BOOKING_DESIGNATOR"),
                    col("sourceCode").as("SOURCE_CODE"),
                    col("kilometresBetweenAirportPairs").as("KILOMETRES_BETWEEN_AIRPORT_PAIRS")
                )
                .coalesce(4).write().mode(parquetSaveMode).parquet(AppConfigService.getConfig("dds.parquet.dir.raw") + "/dds.parquet");


        // 3. Processing EDS trailer
        // Note: This could be improved to get last DataFrame record.
        JavaRDD<DDSTrailerRecord> trailerRDDs = fileContent.filter(line -> {
            boolean isTrailer = false;
            if (StringUtils.isNotEmpty(line) && line.charAt(0) == 'Z') {
                isTrailer = true;
            }

            return isTrailer;
        }).map(row -> {
            DDSTrailerRecord result = new DDSTrailerRecord();

            result.setGlobalUID(globalUID);
            result.setRecordCount(StringUtils.getNumbericValueAsInteger(row.substring(1, 10)));
            result.setFiller(row.substring(10)); // until offset 463 but to be safe then getting from offset 10

            //System.out.println(result);

            return result;
        });

        sqlContext.createDataFrame(trailerRDDs, DDSTrailerRecord.class)
                .select(
                        col("globalUID").as("GUID"),
                        col("recordId").as("RECORD_IDENTIFIER"),
                        col("recordCount").as("RECORD_COUNT"),
                        col("filler").as("FILLER")
                )
                .write().mode(parquetSaveMode).parquet(AppConfigService.getConfig("dds.parquet.dir.raw") + "/dds_trailer.parquet");


        // 4. Write metadata record
        List<DDSRecordMetaData> headerRecordList = new ArrayList<DDSRecordMetaData>();

        DDSRecordMetaData record = new DDSRecordMetaData();
        record.setGlobalUID(globalUID);
        record.setSource(rawTextFile);

        headerRecordList.add(record);

        sqlContext.createDataFrame(headerRecordList, DDSRecordMetaData.class)
                .select(
                        col("globalUID").as("GUID"),
                        col("transId").as("TRANS_ID"),
                        col("provider").as("PROVIDER"),
                        col("subject").as("SUBJECT"),
                        col("source").as("SOURCE"),
                        col("createdDate").as("DATE_CREATED"),
                        col("createdBy").as("CREATED_BY"),
                        col("createdBySourceAppId").as("CREATED_BY_SRCE_APPL_ID"),
                        col("contentFormatVersion").as("CONTENT_FORMAT_VERSION")

//                        col("createdBySourceUserId").as("CREATED_BY_SRCE_USER_ID"),
//                        col("modifiedBy").as("MODIFIED_BY"),
//                        col("modifiedBySourceUserId").as("MODIFIED_BY_SRCE_USER_ID"),
//                        col("modifiedBySourceAppId").as("MODIFIED_BY_SRCE_APPL_ID"),
//                        col("modifiedDate").as("DATE_MODIFIED")
                )
                .write()
                .mode(parquetSaveMode)
                .parquet(AppConfigService.getConfig("dds.parquet.dir.raw") + "/dds_metadata.parquet");
    }

}
