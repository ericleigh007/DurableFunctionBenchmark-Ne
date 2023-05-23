using System;
using System.IO;
using Newtonsoft.Json;
using System.Diagnostics;
using System.IO.Compression;
using System.Text;

namespace DurableFunctionBenchmark
{
    public class CompressedObject<T>
    {
        public CompressedObject(T input, CompressionLevel compressionLevel)
        {
            var sw = new Stopwatch();
            sw.Start();

            _compressionLevel = compressionLevel;

            _JsonContent = JsonConvert.SerializeObject(input);

            var inputBytes = Encoding.UTF8.GetBytes(_JsonContent);

            var outputBytes = GZipBytes(inputBytes);

            _compressedContent = Convert.ToBase64String(outputBytes);

            sw.Stop();
            _compressTime = sw.Elapsed;
        }

        public static CompressedObject<T> Create(T input, CompressionLevel compressionLevel = CompressionLevel.Optimal)
        {
            return new CompressedObject<T>(input, compressionLevel);
        }

        public U Get<U>()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            var compressedBytes = Convert.FromBase64String(_compressedContent);

            var uncompressedBytes = UnGZipBytes(compressedBytes);

            _JsonContent = Encoding.UTF8.GetString(uncompressedBytes);

            sw.Stop();
            _uncompressTime = sw.Elapsed;

            return JsonConvert.DeserializeObject<U>(_JsonContent);
        }

        private byte[] GZipBytes(byte[] input)
        {
            using (var outputStream = new MemoryStream())
            {
                using (var gzipStream = new GZipStream(outputStream, CompressionMode.Compress))
                {
                    gzipStream.Write(input, 0, input.Length);
                }

                return outputStream.ToArray();
            }
        }

        private byte[] UnGZipBytes(byte[] input)
        {
            using (var outputStream = new MemoryStream())
            {
                using (var inputStream = new MemoryStream(input))
                {
                    using (var gzipStream = new GZipStream(inputStream, CompressionMode.Decompress))
                    {
                        inputStream.Position = 0;
                        gzipStream.CopyTo(outputStream);
                    }

                    outputStream.Position = 0;
                    return outputStream.ToArray();
                }
            }
        }

        [JsonIgnore]
        public TimeSpan CompressTime
        {
            get
            {
                return _compressTime;
            }
        }

        [JsonIgnore]
        public TimeSpan UnCompressTime
        {
            get
            {
                return _uncompressTime;
            }
        }

        [JsonIgnore]
        public CompressionLevel CompressionLevel
        {
            get
            {
                return _compressionLevel;
            }
        }

        [JsonIgnore]
        public int UnCompressedLength
        {
            get
            {
                return _JsonContent.Length;
            }
        }

        [JsonIgnore]
        public int CompressedLength
        {
            get
            {
                return _compressedContent.Length;
            }
        }

        [JsonIgnore]
        public double CompressionFactor
        {
            get 
            {
                return UnCompressedLength != 0 ? (double)CompressedLength / (double)UnCompressedLength : 1.0;
            }
        }

        [JsonProperty("CompressTime")]
        private TimeSpan _compressTime;

        [JsonProperty("CompressedContent")]
        private string _compressedContent;

        [JsonIgnore]
        private TimeSpan _uncompressTime;

        [JsonIgnore]
        private string _JsonContent;

        private CompressionLevel _compressionLevel;
    }
}
