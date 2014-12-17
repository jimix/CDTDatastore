//
//  DatastoreManagerTests.m
//  Tests
//
//  Created by Rhys Short on 17/12/2014.
//
//

#import <Foundation/Foundation.h>
#import <CloudantSync.h>
#import "CloudantSyncTests.h"

@interface DatastoreManagerTests : CloudantSyncTests

@end

@implementation DatastoreManagerTests



- (void)testList5Datastores {
    
    NSArray * array = @[@"datastore0",@"datastore1",@"datastore2",@"datastore3",@"datastore4"];
    
    for(NSString * dsName in array){
        [self.factory datastoreNamed:dsName error:nil];
    }
    
    NSArray * datastores = [self.factory listDatastores];
    STAssertEquals((NSUInteger)5, [datastores count],
                   @"Wrong number of datastores returned, expected 5 got %d",
                   [datastores count]);
    
    for(NSString * dsname in array){
        STAssertTrue([datastores containsObject:dsname], @"Object missing from datastores list");
    }
    
}

- (void) testListDatastoresWithSlash {
    
    [self.factory datastoreNamed:@"adatabase/withaslash" error:nil];
    NSArray * datastores = [self.factory listDatastores];
    STAssertEquals((NSUInteger)1, [datastores count],
                   @"Wrong number of datastores returned, expected 1 got %d",
                   [datastores count]);
    STAssertEqualObjects(@"adatabase/withaslash", [datastores objectAtIndex:0],@"Ds names do not macth");
    
}

-(void) testListEmptyDatastores {
    NSArray * datastores = [self.factory listDatastores];
    STAssertEquals((NSUInteger)0, [datastores count],
                   @"Wrong number of datastores returned, expected 0 got %d",
                   [datastores count]);

}

@end


