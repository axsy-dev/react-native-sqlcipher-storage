using Microsoft.ReactNative;
using Microsoft.ReactNative.Managed;

namespace react_native_sqlcipher_storage
{
    public sealed class ReactPackageProvider : IReactPackageProvider
    {
        public void CreatePackage(IReactPackageBuilder packageBuilder)
        {
           packageBuilder.AddReflectionReactPackageProvider<ReactPackageProvider>();            
        }
    }
}
