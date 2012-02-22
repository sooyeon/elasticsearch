package org.apache.lucene.search.vectorhighlight;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.TokenGroup;

public class MWSimpleHTMLFormatter implements Formatter
{

    private static final String DEFAULT_PRE_TAG = "<B>";
    private static final String DEFAULT_POST_TAG = "</B>";

    private String preTag;
    private String postTag;
    private List<String> terms = new ArrayList<String>();

    public MWSimpleHTMLFormatter(String preTag, String postTag)
    {
        this.preTag = preTag;
        this.postTag = postTag;
    }

    /** Default constructor uses HTML: &lt;B&gt; tags to markup terms. */
    public MWSimpleHTMLFormatter()
    {
        this(DEFAULT_PRE_TAG, DEFAULT_POST_TAG);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.lucene.search.highlight.Formatter#highlightTerm(java.lang.
     * String, org.apache.lucene.search.highlight.TokenGroup)
     */
    public String highlightTerm(String originalText, TokenGroup tokenGroup)
    {
        if (tokenGroup.getTotalScore() <= 0) {
            return originalText;
        }

        // Allocate StringBuilder with the right number of characters from the
        // beginning, to avoid char[] allocations in the middle of appends.
        StringBuilder returnBuffer = new StringBuilder(preTag.length() + originalText.length()
                + postTag.length());
        returnBuffer.append(preTag);
        returnBuffer.append(originalText);
        returnBuffer.append(postTag);
        terms.add(originalText);
        return returnBuffer.toString();
    }

    public List<String> getTerms()
    {
        return terms;
    }

}
