using System.Security.Cryptography;

namespace DotNet.Utils
{
    public static class Md5Extensions
    {
        public static string ToMd5Hash(this byte[] bytes)
        {
            var hashBytes = MD5.HashData(bytes);
            return Convert.ToHexString(hashBytes).ToLower();
        }
    }
}
