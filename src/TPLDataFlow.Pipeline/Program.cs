using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks.Dataflow;

namespace TPLDataFlow.Pipeline
{
    class Program
    {
        static void Main(string[] args)
        {
            int counter1 = 0,
                counter2 = 0,
                counter3 = 0,
                counter4 = 0,
                counter5 = 0,
                counter6 = 0;

            #region Create the members of the pipeline.

            // 1. Buffers the input.
            var buffer = new BufferBlock<string>(
                new DataflowBlockOptions
                {
                    BoundedCapacity = 10,
                    //MaxMessagesPerTask = 1,
                });

            // 2. Downloads the requested resource.
            var downloader = new TransformBlock<string, Stream>(async uri =>
            {
                Console.WriteLine($"{++counter2} - Downloading '{uri}'...");

                return await new HttpClient().GetStreamAsync(uri);
            },
            new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = 1,
                //SingleProducerConstrained = true,
                //MaxMessagesPerTask = 1,
                //MaxDegreeOfParallelism = 1
            });

            // 3. Decodes the stream content.
            var decoder = new TransformBlock<Stream, string>(async stream =>
            {
                Console.WriteLine($"{++counter3} - Decoding ...");

                using var gzipStream = new GZipStream(stream, CompressionMode.Decompress);
                using var reader = new StreamReader(gzipStream);
                return await reader.ReadToEndAsync();
            });

            // 4. Separates the specified text into an array of words.
            var parser = new TransformBlock<string, string[]>(text =>
            {
                Console.WriteLine($"{++counter4} - Parsing ...");

                // Remove common punctuation by replacing all non-letter characters 
                // with a space character.
                char[] tokens = text.Select(c => char.IsLetter(c) ? c : ' ').ToArray();
                text = new string(tokens);

                // Separate the text into an array of words.
                return text.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            });

            // 5. Removes short words and duplicates.
            var filter = new TransformBlock<string[], string[]>(words =>
            {
                Console.WriteLine($"{++counter5} - Filtering ...");

                return words
                   .Where(word => word.Length > 3)
                   .Distinct()
                   .ToArray();
            });

            // Finds all words in the specified collection whose reverse also 
            // exists in the collection.
            var reversedFinder = new TransformManyBlock<string[], string>(words =>
            {
                Console.WriteLine($"{++counter6} - Finding reversed words ...");

                var wordsSet = new HashSet<string>(words);

                return from word in words.AsParallel()
                       let reverse = new string(word.Reverse().ToArray())
                       where word != reverse && wordsSet.Contains(reverse)
                       select word;
            });

            // 7. Prints the provided reversed words to the console.    
            var print = new ActionBlock<string>(reversedWord =>
            {
                Debug.WriteLine($"{counter6} - Printing reversed {reversedWord}/{new string(reversedWord.Reverse().ToArray())}...");
                // Console.WriteLine($"{counter6} - Printing reversed {reversedWord}/{new string(reversedWord.Reverse().ToArray())}...");
            });

            #endregion

            #region Connect the dataflow blocks to form a pipeline.

            var linkOptions = new DataflowLinkOptions { PropagateCompletion = true };

            buffer.LinkTo(downloader, linkOptions);
            downloader.LinkTo(decoder, linkOptions);
            decoder.LinkTo(parser, linkOptions);
            parser.LinkTo(filter, linkOptions);
            filter.LinkTo(reversedFinder, linkOptions);
            reversedFinder.LinkTo(print, linkOptions);

            #endregion

            // Enqueue URL to buffer
            foreach (var uri in File.ReadAllLines("resources.txt"))
            {
                // enqueue sync
                //if (buffer.Post(uri))
                {
                    Console.WriteLine($"{++counter1} Enqueue '{uri}'...");
                };

                // enqueue async
                buffer.SendAsync(uri).GetAwaiter().GetResult();
            }

            // Mark the head of the pipeline as complete.
            buffer.Complete();

            // Wait for the last block in the pipeline to process all messages.
            print.Completion.Wait();
        }
    }
}
