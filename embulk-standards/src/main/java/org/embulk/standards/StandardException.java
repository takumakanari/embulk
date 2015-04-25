package org.embulk.standards;

public class StandardException {

    public static class InvalidFormatException
            extends RuntimeException
    {
        public InvalidFormatException(String message)
        {
            super(message);
        }
    }

    public static class TooManyColumnsException
            extends InvalidFormatException
    {
        public TooManyColumnsException(String message)
        {
            super(message);
        }
    }

    public static class TooFewColumnsException
            extends InvalidFormatException
    {
        public TooFewColumnsException(String message)
        {
            super(message);
        }
    }

}
