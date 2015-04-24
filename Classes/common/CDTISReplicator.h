//
//  CDTISReplicator.h
//  Pods
//
//  Created by Jimi Xenidis on 4/24/15.
//
//

#import <CoreData/CoreData.h>
#import <CloudantSync.h>

#import "CDTConflictResolver.h"
#import "CDTDatastore+Conflicts.h"

@class CDTIncrementalStore;
@interface CDTISReplicator : NSObject <CDTConflictResolver>


@property (nonatomic, strong) CDTReplicator *replicator;

- (NSArray *)processConflictsWithContext:(NSManagedObjectContext *)context error:(NSError **)error;

- (instancetype)initWithDatastore:(CDTDatastore *)datastore
                 incrementalStore:(CDTIncrementalStore *)incrementalStore
                       replicator:(CDTReplicator *)replicator;

@end