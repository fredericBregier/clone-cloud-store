Using fake Streams in tests
####################################

Often we need to have a Fake InputStream or a Fake OutputStream, without having to generate a Stream fully in memory.

.. code-block:: java
  :caption: Example of usage of **FakeInputStream** **VoidOutputStream** in a test

    @Test
    void createConsumeInputOutputStream() {
       long length = x;
       try (InputStream inputStream = new FakeInputStream(length);  // Return a Fake InputStream with random content
            OutputStream outputStream = new VoidOutputStream()) {   // DevNull OutputStream
          assertEquals(length, inputStream.transferTo(outputStream));
       }
       try (InputStream inputStream = new FakeInputStream(length, (byte) 'A');  // Content will be fill with 'A'
            OutputStream outputStream = new VoidOutputStream()) {   // DevNull OutputStream
          assertEquals(length, inputStream.transferTo(outputStream));
       }
    }
  }

