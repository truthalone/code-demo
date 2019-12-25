package utils;

public class CSVLineReader
{
    private String line;
    private int curPos;
    private int maxPos;

    public CSVLineReader(String csvLine)
    {
        line = csvLine;
        curPos = 0;
        maxPos = line.length();
    }

    public int getSize()
    {
        int i = 0;
        int size = 1;
        while((i = nextPosition(i)) < maxPos)
        {
            i++;
            size++;
        }
        return size;
    }

    public boolean hasMoreItem()
    {
        return (nextPosition(curPos) <= maxPos);
    }

    public String nextItem()
    {
        String item = nextCompItem();
        if(item != null)
        {
            return item.trim();
        }
        return null;
    }


    public String nextCompItem()
    {
        if(curPos > maxPos)
        {
            return null;
        }
        int pos = curPos;
        curPos = nextPosition(curPos);

        StringBuffer buffer = new StringBuffer();
        while(pos < curPos)
        {
            char ch = line.charAt(pos);
            pos++;
            if(ch == '"')
            {
                if((pos < curPos) && (line.charAt(pos) == '"'))
                {
                    buffer.append(ch);
                    pos++;
                }
            }
            else
            {
                buffer.append(ch);
            }
        }

        curPos++;
        return new String(buffer);
    }

    private int nextPosition(int pos)
    {
        boolean isQuote = false;

        while (pos < maxPos)
        {
            char ch = line.charAt(pos);
            if(!isQuote && ch == ',')
            {
                break;
            }
            else if('"' == ch)
            {
                isQuote = !isQuote;
            }
            pos++;
        }

        return pos;
    }
}
