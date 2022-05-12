namespace Converter
{
    using System;
    using System.Runtime.InteropServices;

    public class SystemConsole : IDisposable
  {
    [DllImport("kernel32.dll", EntryPoint = "AllocConsole", SetLastError = true, CharSet = CharSet.Auto, CallingConvention = CallingConvention.StdCall)]
    private static extern int AllocConsole();

    [DllImport("kernel32.dll", EntryPoint = "FreeConsole", SetLastError = true, CharSet = CharSet.Auto, CallingConvention = CallingConvention.StdCall)]
    public static extern int FreeConsole();

    public SystemConsole()
    {
      AllocConsole();
    }

    public void Dispose()
    {
      FreeConsole();
    }

    public static IDisposable Create()
    {
      return new SystemConsole();
    }
  }
}
