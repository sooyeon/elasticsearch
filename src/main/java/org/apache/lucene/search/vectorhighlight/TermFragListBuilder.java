package org.apache.lucene.search.vectorhighlight;

import java.util.Arrays;

import org.apache.lucene.search.vectorhighlight.FieldPhraseList.WeightedPhraseInfo;

/**
 * Simply returns matched term offsets with individual boost values
 * This method disregards the fragCharSize
 *
 * @author Soo Yeon Lee <sooyeon@meltwater.com>
 *
 */
public class TermFragListBuilder implements FragListBuilder
{

    public FieldFragList createFieldFragList(FieldPhraseList fieldPhraseList, int fragCharSize)
    {
        FieldFragList ffl = new FieldFragList(fragCharSize);

        for (WeightedPhraseInfo wpi : fieldPhraseList.phraseList) {
            ffl.add(wpi.getStartOffset(), wpi.getEndOffset(), Arrays.asList(wpi));
        }
        return ffl;
    }
}
