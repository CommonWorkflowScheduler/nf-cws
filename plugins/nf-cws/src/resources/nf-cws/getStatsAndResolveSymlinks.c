#include <dirent.h>
#include <fts.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define FULL_DESCR 0
#define SHORT_DESCR_WITH_TIMESTAMP 1

#define INFILES_NAME "infiles"
#define OUTFILES_NAME "outfiles"

#define STACK_MIN_SIZE 1

#define SYMLINK_TARGET_BUFFER_SIZE 500

struct symlink {
    char * src;
    char * dst;
};

struct stack {
    int top;
    int size;
    struct symlink * items;
};

struct stack * newStack(int size) {
    struct stack * ptr = (struct stack *) malloc(sizeof(struct stack *));
    ptr->top = -1;
    ptr->items = (struct symlink *) malloc(sizeof(struct symlink) * size);
    ptr->size = size;
    return ptr;
}

int isEmpty(struct stack * ptr) {
    return (ptr->top == -1);
}

struct stack * push_symlink(struct stack * ptr, char * const src, char * const dst) {
    if (ptr->top + 1 == ptr->size) {
        struct stack * new_ptr = newStack(ptr->size * 2);
        new_ptr->items = (struct symlink *) realloc(ptr->items, sizeof(struct symlink) * ptr->size * 2);
        new_ptr->top = ptr->top;
        free(ptr);
        ptr = new_ptr;
    }
    ptr->top++;
    char * source = (char *) malloc(strlen(src) + 1);
    strcpy(source, src);
    char * destination = (char *) malloc(strlen(dst) + 1);
    strcpy(destination, dst);
    ptr->items[ptr->top].src = source;
    ptr->items[ptr->top].dst = destination;
    return ptr;
}

const struct symlink * const head(struct stack * ptr) {
    if (isEmpty(ptr)) {
        fprintf(stderr, "Error: Stack is empty!");
        exit(EXIT_FAILURE);
    }
    return &(ptr->items[ptr->top]);
}

void delete_top(struct stack * ptr) {
    if (isEmpty(ptr)) {
        fprintf(stderr, "Error: Stack is empty!");
        exit(EXIT_FAILURE);
    }
    free(ptr->items[ptr->top].src);
    free(ptr->items[ptr->top].dst);
    ptr->top--;
}

void deleteStack(struct stack * ptr) {
    while (!isEmpty(ptr)) {
        delete_top(ptr);
    }
    ptr->size = -1;
    free(ptr->items);
    free(ptr);
}

int collectFileInformation(char * const * dir_to_search, const int version,
    const char * const local_dir,
    const char * const result_filename);
int getFullDescr(char * const * dir,
    const char * const local_dir,
    FILE * file_ptr);
int getShortDescrAndTimestamp(char * const * dir,
    const char * const local_dir,
    FILE * file_ptr);


int main(int arc, char * const argv[]) {

    if (arc < 5) {
        fprintf(stderr, "Error: too few arguments!\n");
        return -1;
    }
    argv++;
    struct timespec start_time, end_time;
    int gettimeofday_rc;
    if ((gettimeofday_rc = clock_gettime(CLOCK_REALTIME, &start_time)) != 0) {
        fprintf(stderr, "Error getting the time of day\n");
        return gettimeofday_rc;
    }

    if (strcmp(argv[0], INFILES_NAME) != 0 && strcmp(argv[0], OUTFILES_NAME) != 0) {
        fprintf(stderr, "Error: version must be '%s' or '%s'\n", INFILES_NAME, OUTFILES_NAME);
        return -1;
    }
    const char * const name = argv++[0];
    const int version = strcmp(name, INFILES_NAME) == 0
        ? SHORT_DESCR_WITH_TIMESTAMP : FULL_DESCR;
    const char * const result_filename = argv++[0];
    const char * const local_dir = argv++[0];
    int rc = collectFileInformation(argv, version, local_dir, result_filename);

    if ((gettimeofday_rc = clock_gettime(CLOCK_REALTIME, &end_time)) != 0) {
        fprintf(stderr, "Error getting the time of day\n");
        return gettimeofday_rc;
    }
    long long int start = start_time.tv_sec * 1000000000 + start_time.tv_nsec;
    long long int end = end_time.tv_sec * 1000000000 + end_time.tv_nsec;
    printf("%lli\n", (end - start) / 1000000);

    return rc;
}

int collectFileInformation(char * const * dir_to_search, const int version,
    const char * const local_dir, 
    const char * const result_filename) {

    DIR* dir_ptr = opendir(local_dir);
    if (dir_ptr) {
        closedir(dir_ptr);
    } else {
        fprintf(stderr, "Error: the local directory '%s' does not exist.\n", local_dir);
        return -1;
    }

    dir_ptr = opendir(dir_to_search[0]);
    if (dir_ptr) {
        closedir(dir_ptr);
    } else {
        fprintf(stderr, "Error: the directory to search '%s' does not exist.\n", dir_to_search[0]);
        return -1;
    }

    FILE * file_ptr = fopen(result_filename, "w");
    if (file_ptr == NULL) {
        fprintf(stderr, "Error opening the file %s\n", result_filename);
        return -1;
    }
    int rc;
    switch (version) {
        case FULL_DESCR:
            rc = getFullDescr(dir_to_search, local_dir, file_ptr);
            break;
        case SHORT_DESCR_WITH_TIMESTAMP:
            rc = getShortDescrAndTimestamp(dir_to_search, local_dir, file_ptr);
            break;
        
        default:
            break;
    }
    fclose(file_ptr);
    return rc;
}

int getFullDescr(char * const * dir,
    const char * const local_dir,
    FILE * file_ptr) {

    FTS * fts_ptr;
    FTSENT * ptr, * ch_ptr;
    int fts_options = FTS_PHYSICAL;

    if ((fts_ptr = fts_open(dir, fts_options, NULL)) == NULL) {
        fprintf(stderr, "Error traversing the directory %s\n", dir[0]);
        return -1;
    }
    ch_ptr = fts_children(fts_ptr, 0);
    if (ch_ptr == NULL) {
        return 0;
    }
    int skip_next = 0;
    struct stack * symlink_stack = newStack(STACK_MIN_SIZE);
    char * symlink_target_path = (char *) malloc(SYMLINK_TARGET_BUFFER_SIZE);
    while ((ptr = fts_read(fts_ptr)) != NULL) {
        if (ptr->fts_info == FTS_DP) {
            continue;
        }
        if (skip_next) {
            skip_next = 0;
            continue;
        }
        char * file_type;
        strcpy(symlink_target_path, "");
        int exists;
        switch (ptr->fts_info) {
            case FTS_D:
                file_type = "directory";
                exists = 1;
                break;
            case FTS_F:
                file_type = "regular file";
                exists = 1;
                break;
            case FTS_SL:
                file_type = "symbolic link";
                realpath(ptr->fts_path, symlink_target_path);
                if (symlink_target_path == NULL) {
                    strcpy(symlink_target_path, "");
                }
                exists = 1 + access(symlink_target_path, F_OK); // test for file existence
                if (!exists) {
                    break;
                }
                // checking whether it is a directory:
                struct stat target_file_stat;
                int stat_rv;
                if ((stat_rv = stat(symlink_target_path, &target_file_stat)) != 0) {
                    fprintf(stderr, "Error reading the file %s\n", symlink_target_path);
                    return stat_rv;
                }
                if (S_ISDIR(target_file_stat.st_mode)) {
                    // if target is not local, we skip it
                    if (strncmp(symlink_target_path, local_dir, strlen(local_dir)) != 0) {
                        file_type = "non-local directory";
                        break;
                    }
                    file_type = "directory";
                    // if target is within the directory we are searching, we skip it,
                    // to prevent searching a directory more than once
                    if (strncmp(symlink_target_path, dir[0], strlen(dir[0])) == 0) {
                        break;
                    }
                    fts_set(fts_ptr, ptr, FTS_FOLLOW);
                    symlink_stack = push_symlink(symlink_stack, ptr->fts_path, symlink_target_path);
                    skip_next = 1;
                } else if (S_ISREG(target_file_stat.st_mode)) {
                    file_type = "regular file";
                }
                break;

            default:
                file_type = "unknown";
                exists = 1;
                break;
        }
        if (!isEmpty(symlink_stack) && strcmp(file_type, "symbolic link") != 0) {
            while (strncmp(head(symlink_stack)->src, ptr->fts_path, strlen(head(symlink_stack)->src)) != 0) {
                delete_top(symlink_stack);
                if (isEmpty(symlink_stack)) {
                    break;
                }
            }
            if (!isEmpty(symlink_stack)) {
                strcpy(symlink_target_path, head(symlink_stack)->dst);
                int fts_len = strlen(ptr->fts_path);
                int to_replace_len = strlen(head(symlink_stack)->src);
                int rel_len = fts_len - to_replace_len;
                char rel_path[rel_len+1];
                memcpy(rel_path, &ptr->fts_path[to_replace_len], rel_len+1);
                strcat(symlink_target_path, rel_path);
            }
        }
        fprintf(
            file_ptr,
            "%s;%i;%s;%li;%s;%li%09li;%li%09li;%li%09li\n",
            ptr->fts_path,
            exists,
            symlink_target_path,
            ptr->fts_statp->st_size,
            file_type,
            ptr->fts_statp->st_ctim.tv_sec, ptr->fts_statp->st_ctim.tv_nsec,
            // ctim - time of last status change which is used as an approximation of the creation time
            ptr->fts_statp->st_atim.tv_sec, ptr->fts_statp->st_atim.tv_nsec,
            ptr->fts_statp->st_mtim.tv_sec, ptr->fts_statp->st_mtim.tv_nsec
        );
    }
    free(symlink_target_path);
    deleteStack(symlink_stack);
    fts_close(fts_ptr);
    return 0;
}

int getShortDescrAndTimestamp(char * const * dir,
    const char * const local_dir,
    FILE * file_ptr) {

    struct timespec time_now;
    int gettimeofday_rc;
    if ((gettimeofday_rc = clock_gettime(CLOCK_REALTIME, &time_now)) != 0) {
        fprintf(stderr, "Error getting the time of day\n");
        return gettimeofday_rc;
    } 
    fprintf(file_ptr, "%li%li\n", time_now.tv_sec, time_now.tv_nsec);

    fprintf(file_ptr, "%s\n", dir[0]);

    int prefix_len = strlen(dir[0]);
    if (dir[0][prefix_len-1] != '/') {
        prefix_len++;
    }

    FTS * fts_ptr;
    FTSENT * ptr, * ch_ptr;
    int fts_options = FTS_PHYSICAL;

    if ((fts_ptr = fts_open(dir, fts_options, NULL)) == NULL) {
        fprintf(stderr, "Error traversing the directory %s\n", dir[0]);
        return -1;
    }
    ch_ptr = fts_children(fts_ptr, 0);
    if (ch_ptr == NULL) {
        return 0;
    }
    int skip_next = 0;
    struct stack * symlink_stack = newStack(STACK_MIN_SIZE);
    char * symlink_target_path = (char *) malloc(SYMLINK_TARGET_BUFFER_SIZE);
    while ((ptr = fts_read(fts_ptr)) != NULL) {
        if (ptr->fts_info == FTS_DP) {
            continue;
        }
        if (strncmp(ptr->fts_path, dir[0], strlen(ptr->fts_path)) == 0) {
            continue;
        }
        if (skip_next) {
            skip_next = 0;
            continue;
        }
        char * file_type;
        strcpy(symlink_target_path, "");
        int exists;
        switch (ptr->fts_info) {
            case FTS_D:
                file_type = "directory";
                exists = 1;
                break;
            case FTS_F:
                file_type = "regular file";
                exists = 1;
                break;
            case FTS_SL:
                file_type = "symbolic link";
                int bytes_written = readlink(ptr->fts_path, symlink_target_path, SYMLINK_TARGET_BUFFER_SIZE);
                symlink_target_path[bytes_written] = '\0';
                // realpath(ptr->fts_path, symlink_target_path);
                if (symlink_target_path == NULL) {
                    strcpy(symlink_target_path, "");
                }
                exists = 1 + access(symlink_target_path, F_OK); // test for file existence
                if (!exists) {
                    break;
                }
                // checking whether it is a directory:
                struct stat target_file_stat;
                int stat_rv;
                if ((stat_rv = stat(symlink_target_path, &target_file_stat)) != 0) {
                    fprintf(stderr, "Error reading the file %s\n", symlink_target_path);
                    return stat_rv;
                }
                if (S_ISDIR(target_file_stat.st_mode)) {
                    // if target is not local, we skip it
                    if (strncmp(symlink_target_path, local_dir, strlen(local_dir)) != 0) {
                        file_type = "non-local directory";
                        break;
                    }
                    file_type = "directory";
                    // if target is within the directory we are searching, we skip it,
                    // to prevent searching a directory more than once
                    if (strncmp(symlink_target_path, dir[0], strlen(dir[0])) == 0) {
                        break;
                    }
                    fts_set(fts_ptr, ptr, FTS_FOLLOW);
                    symlink_stack = push_symlink(symlink_stack, ptr->fts_path, symlink_target_path);
                    skip_next = 1;
                } else if (S_ISREG(target_file_stat.st_mode)) {
                    file_type = "regular file";
                }
                break;

            default:
                file_type = "unknown";
                exists = 1;
                break;
        }
        if (!isEmpty(symlink_stack) && strcmp(file_type, "symbolic link") != 0) {
            while (strncmp(head(symlink_stack)->src, ptr->fts_path, strlen(head(symlink_stack)->src)) != 0) {
                delete_top(symlink_stack);
                if (isEmpty(symlink_stack)) {
                    break;
                }
            }
            if (!isEmpty(symlink_stack)) {
                strcpy(symlink_target_path, head(symlink_stack)->dst);
                int fts_len = strlen(ptr->fts_path);
                int to_replace_len = strlen(head(symlink_stack)->src);
                int rel_len = fts_len - to_replace_len;
                char rel_path[rel_len+1];
                memcpy(rel_path, &ptr->fts_path[to_replace_len], rel_len+1);
                strcat(symlink_target_path, rel_path);
            }
        }
        fprintf(
            file_ptr,
            "%s;%i;%s;%s\n",
            ptr->fts_path + prefix_len,
            exists,
            symlink_target_path,
            file_type
        );
    }
    free(symlink_target_path);
    deleteStack(symlink_stack);
    fts_close(fts_ptr);
    return 0;
}
