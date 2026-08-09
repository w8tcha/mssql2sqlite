namespace Converter
{
    using System;
    using System.Runtime.InteropServices;

    public sealed class SystemConsole : IDisposable
  {
    [DllImport("kernel32.dll", EntryPoint = "AllocConsole", SetLastError = true, CharSet = CharSet.Auto, CallingConvention = CallingConvention.StdCall)]
    private static extern int AllocConsole();

    [DllImport("kernel32.dll", EntryPoint = "FreeConsole", SetLastError = true, CharSet = CharSet.Auto, CallingConvention = CallingConvention.StdCall)]
    private static extern int FreeConsole();

    public SystemConsole()
    {
      AllocConsole();
    }

    public void Dispose()
    {
      FreeConsole();
      GC.SuppressFinalize(this);
    }

    public static IDisposable Create()
    {
      return new SystemConsole();
    }
  }
}
