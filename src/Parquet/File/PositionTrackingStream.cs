namespace Parquet.File
{
   using System.IO;

   
   class PositionTrackingStream :
      Stream
   {
      readonly Stream _parent;
      long _position;

      public PositionTrackingStream(Stream parent)
      {
         _parent = parent;
      }

      public override bool CanRead => _parent.CanRead;

      public override bool CanSeek => _parent.CanSeek;

      public override bool CanWrite => _parent.CanWrite;

      public override long Length => _parent.Length;

      public override long Position
      {
         get => _position;
         set
         {
            _parent.Position = value;
            _position = value;
         }
      }

      public override void Flush()
      {
         _parent.Flush();
      }

      public override int Read(byte[] buffer, int offset, int count)
      {
         int read = _parent.Read(buffer, offset, count);

         _position += read;

         return read;
      }

      public override long Seek(long offset, SeekOrigin origin)
      {
         long pos = _parent.Seek(offset, origin);
         _position = pos;
         return pos;
      }

      public override void SetLength(long value)
      {
         _parent.SetLength(value);
      }

      public override void Write(byte[] buffer, int offset, int count)
      {
         _parent.Write(buffer, offset, count);
         _position += count;
      }
   }
}
