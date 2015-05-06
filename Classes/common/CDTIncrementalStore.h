//
//  CDTIncrementalStore.h
//
//
//  Created by Jimi Xenidis on 11/18/14.
//
//

#import <CoreData/CoreData.h>
#import <CloudantSync.h>

#import "CDTISReplicator.h"

extern NSString *const CDTISErrorDomain;
extern NSString *const CDTISException;

@interface CDTIncrementalStore : NSIncrementalStore

/**
 *  Returns the string that was used to register this incremental store
 *
 *  @return NSString
 */
+ (NSString *)type;

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
    CDTISErrorReplicationFactory,
    CDTISErrorNotSupported
};

/**
 *  The databaseName is exposed in order to be able to identify the different
 *  CDTIncrementalStore objects. @see +storesFromCoordinator:coordinator
 */
@property (nonatomic, strong) NSString *databaseName;

/**
 *  Create a dictionary of values from the Document Body and Blob Store
 *
 *  @param body      body of document
 *  @param blobStore blobStore attachement dictionary
 *  @param context   context from Core Data
 *  @param version   version
 *
 *  @return dictionary
 */
- (NSDictionary *)valuesFromDocumentBody:(NSDictionary *)body
                           withBlobStore:(NSDictionary *)blobStore
                             withContext:(NSManagedObjectContext *)context
                              versionPtr:(uint64_t *)version;

/**
 *  Get the Managed Object ID
 *
 *  @param name The name of the Entity
 *  @param ref  The object ref
 *
 *  @return The Object ID, may return nil if the either parameter does not exist
 */
- (NSManagedObjectID *)managedObjectIDForEntityName:(NSString *)name
                                    referenceObject:(NSString *)ref;

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
- (CDTISReplicator *)replicatorThatPushesToURL:(NSURL *)remoteURL withError:(NSError **)error;

/**
 * Create a CDTReplicator object set up from replicate changes from a remote database to the
 * local datastore.
 *
 *  @param remoteURL the remote server URL to which the data is replicated.
 *  @param error     report error information
 *
 *  @return a CDTReplicator instance which can be used to start and
 *  stop the replication itself, or `nil` on error.
*/
- (CDTISReplicator *)replicatorThatPullsFromURL:(NSURL *)remoteURL withError:(NSError **)error;

@end
