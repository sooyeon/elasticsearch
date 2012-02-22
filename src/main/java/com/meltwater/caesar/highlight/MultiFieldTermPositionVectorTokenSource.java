package com.meltwater.caesar.highlight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.TermPositionVector;
import org.apache.lucene.index.TermVectorOffsetInfo;
import org.apache.lucene.util.ArrayUtil;

/**
 * Plugin for building correct token stream for multivalued fields
 * with store=no and term_vector_positions_offsets
 * 
 * @author Soo Yeon Lee <sooyeon@meltwater.com>
 *
 */
public class MultiFieldTermPositionVectorTokenSource
{

    /**
     * Constructs a token stream from a term position vector referring to a
     * multivalued field. Position increments are also respected.
     * 
     * @param tpv
     * @param shiftSentence
     *            set to true if highlighting based on a field w/ no term
     *            vectors
     * @return
     */
    public static TokenStream getTokenStream(TermPositionVector tpv, boolean shiftSentence)
    {
        // an object used to iterate across an array of tokens
        final class StoredTokenStream extends TokenStream
        {
            Token tokens[];
            int currentToken;

            CharTermAttribute termAtt;
            OffsetAttribute offsetAtt;
            PositionIncrementAttribute posincAtt;

            StoredTokenStream(Token tokens[])
            {
                this.tokens = tokens;
                termAtt = addAttribute(CharTermAttribute.class);
                offsetAtt = addAttribute(OffsetAttribute.class);
                posincAtt = addAttribute(PositionIncrementAttribute.class);
            }

            @Override
            public boolean incrementToken() throws IOException
            {
                if (currentToken >= tokens.length) {
                    return false;
                }
                Token token = tokens[currentToken++];
                clearAttributes();
                termAtt.setEmpty().append(token);
                offsetAtt.setOffset(token.startOffset(), token.endOffset());
                posincAtt.setPositionIncrement(token.getPositionIncrement());
                return true;
            }
        }

        return new StoredTokenStream(getTokenArray(tpv, shiftSentence));
    }


    /**
     * Constructs a token stream from a term position vector referring to a
     * multivalued field.
     * Position increments are also respected.
     * 
     * @param tpv
     * @return
     */
    public static Token[] getTokenArray(TermPositionVector tpv, boolean shiftSentence)
    {
        // code to reconstruct the original sequence of Tokens
        String[] terms = tpv.getTerms();
        ArrayList<Token> unsortedTokens = new ArrayList<Token>();
        for (int t = 0; t < terms.length; t++) {
            TermVectorOffsetInfo[] offsets = tpv.getOffsets(t);
            if (offsets == null) {
                throw new IllegalArgumentException(
                        "Required TermVector Offset information was not found");
            }
            int[] pos = tpv.getTermPositions(t);
            for (int tp = 0; tp < offsets.length; tp++) {
                int offsetDiff = shiftSentence ? (pos[tp] / 100000) : 0;
                Token token = new Token(terms[t], offsets[tp].getStartOffset() - offsetDiff,
                        offsets[tp].getEndOffset() - offsetDiff);
                token.setPositionIncrement(pos[tp]);
                unsortedTokens.add(token);
            }
        }
        Token tokensInOriginalOrder[] = unsortedTokens.toArray(new Token[0]);
        ArrayUtil.mergeSort(tokensInOriginalOrder, new Comparator<Token>()
        {
            public int compare(Token t1, Token t2)
            {
                if (t1.startOffset() == t2.startOffset())
                    return t1.endOffset() - t2.endOffset();
                else
                    return t1.startOffset() - t2.startOffset();
            }
        });
        return tokensInOriginalOrder;
    }
}
