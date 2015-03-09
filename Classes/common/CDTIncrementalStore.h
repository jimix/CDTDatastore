//
//  CDTIncrementalStore.h
//
//
//  Created by Jimi Xenidis on 11/18/14.
//
//

#import <CoreData/CoreData.h>
#import <CloudantSync.h>

extern NSString *const CDTISErrorDomain;
extern NSString *const CDTISException;

@interface CDTIncrementalStore : NSIncrementalStore

@property (nonatomic, strong) CDTDatastore *datastore;

- (NSInteger)propertyTypeFromDoc:(NSDictionary *)body withName:(NSString *)name;

/**
 *  Returns the string that was used to register this incremental store
 *
 *  @return NSString
 */
+ (NSString *)type;

/**
 *  Returns URL to the local directory that the incremental databases shall be
 *  stored.
 *
 *  @return NSURL
 */
+ (NSURL *)localDir;

/**
 *  Returns an array of @ref CDTIncrementalStore objects associated with a
 *  @ref NSPersistentStoreCoordinator
 *
 *  @param coordinator The coordinator
 *
 *  @return the array
 */
+ (NSArray *)storesFromCoordinator:(NSPersistentStoreCoordinator *)coordinator;

typedef NS_ENUM(NSInteger, CDTIncrementalStoreErrors) {
    CDTISErrorBadURL = 1,
    CDTISErrorBadPath,
    CDTISErrorNilObject,
    CDTISErrorUndefinedAttributeType,
    CDTISErrorObjectIDAttributeType,
    CDTISErrorNaN,
    CDTISErrorRevisionIDMismatch,
    CDTISErrorExectueRequestTypeUnkown,
    CDTISErrorExectueRequestFetchTypeUnkown,
    CDTISErrorMetaDataMismatch,
    CDTISErrorNoRemoteDB,
    CDTISErrorSyncBusy,
    CDTISErrorNotSupported
};

/**
 *  The databaseName is exposed in order to be able to identify the different
 *  CDTIncrementalStore objects. @see +storesFromCoordinator:coordinator
 */
@property (nonatomic, strong) NSString *databaseName;

/**
 * Create a CDTReplicator object set up to replicate changes from the
 * local datastore to a remote database.
 *
 *  @param remoteURL the remote server URL to which the data is replicated.
 *  @param error     report error information
 *
 *  @return a CDTReplicator instance which can be used to start and
 *  stop the replication itself, or `nil` on error.
 */
- (CDTReplicator *)replicatorThatPushesToURL:(NSURL *)remoteURL withError:(NSError **)error;

/**
 * Create a CDTReplicator object set up to replicate changes from a remote database to the
 * local datastore.
 *
 *  @param remoteURL the remote server URL to which the data is replicated.
 *  @param error     report error information
 *
 *  @return a CDTReplicator instance which can be used to start and
 *  stop the replication itself, or `nil` on error.
*/
- (CDTReplicator *)replicatorThatPullsFromURL:(NSURL *)remoteURL withError:(NSError **)error;

/**
 *  <#Description#>
 */
- (NSUInteger)processConflictsWithError:(NSError **)error;

@end
