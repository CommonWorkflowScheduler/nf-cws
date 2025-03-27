package nextflow.cws.wow.file

import groovy.util.logging.Slf4j
import nextflow.extension.FilesEx
import nextflow.file.FileHelper
import nextflow.file.FilePatternSplitter

import java.nio.file.FileSystemLoopException
import java.nio.file.FileVisitResult
import java.nio.file.FileVisitOption
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes

@Slf4j
class WOWFileHelper extends FileHelper {

    static Path fakePath(Path path, Path destination ) {
        String pathHelper = path.toString()
        String n1 = FilesEx.getName(destination.getParent())
        String n2 = FilesEx.getName(destination)
        String cdir = (n1 as Path).resolve(n2).toString()

        int index = pathHelper.indexOf( cdir )

        if( index == -1 ){
            log.error("Cannot calculate fake path for path: $path to dest.: $destination cdir: $cdir")
            return path
        }

        return destination.getParent().getParent().resolve(pathHelper.substring( index ))
    }

    static void visitFiles( Map options = null, Path folder, String filePattern, Closure action ) {
        assert folder
        assert filePattern
        assert action
        if (options == null) options = Map.of()
        final type = options.type ?: 'any'
        final walkOptions = options.followLinks == false ? EnumSet.noneOf(FileVisitOption.class) : EnumSet.of(FileVisitOption.FOLLOW_LINKS)
        final int maxDepth = getMaxDepth(options.maxDepth, filePattern)
        final includeHidden = options.hidden as Boolean ?: filePattern.startsWith('.')
        final includeDir = type in ['dir', 'any']
        final includeFile = type in ['file', 'any']
        final syntax = options.syntax ?: 'glob'
        final relative = options.relative == true
        final matcher = getPathMatcherFor("$syntax:${filePattern}", folder.fileSystem)
        final singleParam = action.getMaximumNumberOfParameters() == 1

        final boolean outFileExists = new File(folder.resolve(".command.outfiles").toString()).exists()

        def visitor = new SimpleFileVisitor<Path>() {

            @Override
            FileVisitResult preVisitDirectory(Path fullPath, BasicFileAttributes attrs) throws IOException {
                fullPath = outFileExists ? fakePath(fullPath, folder) : fullPath
                final int depth = fullPath.nameCount - folder.nameCount
                final path = relativize0(folder, fullPath)
                log.trace "visitFiles > dir=$path; depth=$depth; includeDir=$includeDir; matches=${matcher.matches(path)}; isDir=${attrs.isDirectory()}"
                if (depth > 0 && includeDir && matcher.matches(path) && attrs.isDirectory() && (includeHidden || !isHidden(fullPath))) {
                    def result = relative ? path : fullPath
                    singleParam ? action.call(result) : action.call(result, attrs)
                }
                return depth > maxDepth ? FileVisitResult.SKIP_SUBTREE : FileVisitResult.CONTINUE
            }

            @Override
            FileVisitResult visitFile(Path fullPath, BasicFileAttributes attrs) throws IOException {
                final Path fakePath = outFileExists ? fakePath(fullPath, folder) : null
                final path = folder.relativize(fakePath ?: fullPath)
                log.trace "visitFiles > file=$path; includeFile=$includeFile; matches=${matcher.matches(path)}; isRegularFile=${attrs.isRegularFile()}"

                if (includeFile && matcher.matches(path) && (attrs.isRegularFile() || (options.followLinks == false && attrs.isSymbolicLink())) && (includeHidden || !isHidden(fullPath))) {
                    def result = relative ? path : fullPath
                    singleParam ? action.call(result) : action.call(result, attrs)
                }
                return FileVisitResult.CONTINUE
            }

            FileVisitResult visitFileFailed(Path currentPath, IOException e) {
                if (e instanceof FileSystemLoopException) {
                    final Path fakePath = outFileExists ? fakePath(currentPath, folder) : null
                    final path = folder.relativize(fakePath ?: currentPath).toString()
                    final capture = FilePatternSplitter.glob().parse(filePattern).getParent()
                    final message = "Circular file path detected -- Files in the following directory will be ignored: $currentPath"
                    // show a warning message only when offending path is contained
                    // by the capture path specified by the user
                    if (capture == './' || path.startsWith(capture))
                        log.warn(message)
                    else
                        log.debug(message)
                    return FileVisitResult.SKIP_SUBTREE
                }
                throw e
            }
        }

        if (outFileExists) {
            LocalFileWalker.walkFileTree(folder, walkOptions, Integer.MAX_VALUE, visitor, folder)
        } else {
            Files.walkFileTree(folder, walkOptions, Integer.MAX_VALUE, visitor)
        }
    }
}
