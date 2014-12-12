//
//  CloudantTests.m
//  Tests
//
//  Created by Rhys Short on 08/10/2014.
//
//

#import "CloudantTests.h"
#import "CDTlogging.h"
#import "DDTTYLogger.h"

@implementation CloudantTests

+ (void)initialize
{
    
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        [DDLog addLogger:[DDTTYLogger sharedInstance]];
        CDTChangeLogLevel(CDTINDEX_LOG_CONTEXT, DDLogLevelWarning);
        CDTChangeLogLevel(CDTREPLICATION_LOG_CONTEXT, DDLogLevelWarning);
        CDTChangeLogLevel(CDTDATASTORE_LOG_CONTEXT, DDLogLevelWarning);
        CDTChangeLogLevel(CDTDOCUMENT_REVISION_LOG_CONTEXT, DDLogLevelWarning);
        CDTChangeLogLevel(CDTTD_REMOTE_REQUEST_CONTEXT, DDLogLevelWarning);
        CDTChangeLogLevel(CDTTD_JSON_CONTEXT, DDLogLevelWarning);
    });
}

@end
