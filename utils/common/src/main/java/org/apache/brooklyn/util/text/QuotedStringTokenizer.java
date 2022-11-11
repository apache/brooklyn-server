/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.util.text;

import org.apache.brooklyn.util.exceptions.Exceptions;

import java.io.IOException;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

/** As 'StringTokenizer' but items in quotes (single or double) are treated as single tokens
 * (cf mortbay's QuotedStringTokenizer) 
 */  
public class QuotedStringTokenizer {

    final StringTokenizer delegate;
    final String quoteChars;
    final boolean includeQuotes;
    final String delimiters;
    final boolean includeDelimiters;
    /** see the static parseXxxx methods for explanation */
    final boolean expectQuotesDelimited;
    final boolean failOnOpenQuote;

    public static String DEFAULT_QUOTE_CHARS = "\"\'";


    protected String DEFAULT_QUOTE_CHARS() {
        return DEFAULT_QUOTE_CHARS;
    }
    
    public final static String DEFAULT_DELIMITERS = " \t\n\r\f";    
    
    /** default quoted tokenizer, using single and double quotes as quote chars and returning quoted results
     * (use unquoteToken to unquote), and using whitespace chars as delimeters (not included as tokens);
     * string may be null if the nothing will be tokenized and the class is used only for
     * quoteToken(String) and unquote(String).
     */
    public QuotedStringTokenizer(String stringToTokenize) {
        this(stringToTokenize, true);
    }
    public QuotedStringTokenizer(String stringToTokenize, boolean includeQuotes) {
        this(stringToTokenize, null, includeQuotes);
    }
    public QuotedStringTokenizer(String stringToTokenize, String quoteChars, boolean includeQuotes) {
        this(stringToTokenize, quoteChars, includeQuotes, null, false);
    }

    public QuotedStringTokenizer(String stringToTokenize, String quoteChars, boolean includeQuotes, String delimiters, boolean includeDelimiters) {
        this(stringToTokenize, quoteChars, includeQuotes, delimiters, includeDelimiters, false, false);
    }
    public QuotedStringTokenizer(String stringToTokenize, String quoteChars, boolean includeQuotes, String delimiters, boolean includeDelimiters, boolean expectQuotesDelimited, boolean failOnOpenQuote) {
        delegate = new StringTokenizer(stringToTokenize==null ? "" : stringToTokenize, (delimiters==null ? DEFAULT_DELIMITERS : delimiters), true);
        this.quoteChars = quoteChars==null ? DEFAULT_QUOTE_CHARS() : quoteChars;
        this.includeQuotes = includeQuotes;
        this.delimiters = delimiters==null ? DEFAULT_DELIMITERS : delimiters;
        this.includeDelimiters = includeDelimiters;
        this.expectQuotesDelimited = expectQuotesDelimited;
        this.failOnOpenQuote = failOnOpenQuote;
        updateNextToken();
    }
    
    public static class Builder {
        private String quoteChars = DEFAULT_QUOTE_CHARS;
        private boolean includeQuotes=true;
        private String delimiterChars=DEFAULT_DELIMITERS;
        private boolean includeDelimiters=false;
        private boolean expectQuotesDelimited=false;
        private boolean failOnOpenQuote=false;

        public QuotedStringTokenizer build(String stringToTokenize) {
            return new QuotedStringTokenizer(stringToTokenize, quoteChars, includeQuotes, delimiterChars, includeDelimiters, expectQuotesDelimited, failOnOpenQuote);
        }
        public List<String> buildList(String stringToTokenize) {
            return new QuotedStringTokenizer(stringToTokenize, quoteChars, includeQuotes, delimiterChars, includeDelimiters, expectQuotesDelimited, failOnOpenQuote).remainderAsList();
        }
        
        public Builder quoteChars(String quoteChars) { this.quoteChars = quoteChars; return this; }
        public Builder addQuoteChars(String quoteChars) { this.quoteChars = this.quoteChars + quoteChars; return this; }
        public Builder includeQuotes(boolean includeQuotes) { this.includeQuotes = includeQuotes; return this; } 
        public Builder delimiterChars(String delimiterChars) { this.delimiterChars = delimiterChars; return this; }
        public Builder addDelimiterChars(String delimiterChars) { this.delimiterChars = this.delimiterChars + delimiterChars; return this; }
        public Builder includeDelimiters(boolean includeDelimiters) { this.includeDelimiters = includeDelimiters; return this; }
        public Builder keepInternalQuotes(boolean expectQuotesDelimited) { this.expectQuotesDelimited = expectQuotesDelimited; return this; }
        public Builder expectQuotesDelimited(boolean expectQuotesDelimited) { this.expectQuotesDelimited = expectQuotesDelimited; return this; }
        public Builder failOnOpenQuote(boolean failOnOpenQuote) { this.failOnOpenQuote = failOnOpenQuote; return this; }
    }
    public static Builder builder() {
        return new Builder();
    }

    /** this is the historic default in Brooklyn, respecting quotes anywhere (processing them before delimiters),
     *  good if quotes are not offset with whitespace (eg x="foo,bar",y="baz" with a , as a delimiter) */
    public static List<String> parseWithQuotesAnywhere(String s) {
        return QuotedStringTokenizer.builder().expectQuotesDelimited(false).buildList(s);
    }

    /** revision to QST to acknowledging quotes only if offset by delimiters;
     * good if delimiters are whitespace, (eg x = "foo bar" y = "baz"); in this mode, fail will cause it to fail if there are any unescaped quotes,
     * but in non-fail mode it will avoid getting confused by an apostrophe as a quote! */
    public static List<String> parseWithDelimitedQuotesOnly(String s) {
        return QuotedStringTokenizer.builder().expectQuotesDelimited(true).buildList(s);
    }

    /** use java StreamTokenizer with most of the defaults, which does most of what we want -- unescaping internal quotes -- and following its usual style of distinguishing words from identifiers etc*/
    public static List<String> parseAsStreamTokenizerIdentifierStrings(String s) {
        try {
            Reader reader = new StringReader(s);
            StreamTokenizer streamTokenizer = new StreamTokenizer(reader);
            streamTokenizer.ordinaryChars('0', '9');
            streamTokenizer.wordChars('0', '9');

            return readAll(streamTokenizer);

        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
    }

    /** use java StreamTokenizer with most of the defaults, which does most of what we want -- unescaping internal quotes -- and following its usual style of distinguishing words from identifiers etc.
     * however there is no way i can see to distinguish quoted fragments with spaces from quoted fragments without spaces around them. */
    public static List<String> parseAsStreamTokenizerWhitespaceOrStrings(String s) {
        try {
            Reader reader = new StringReader(s);
            StreamTokenizer streamTokenizer = new StreamTokenizer(reader);
            streamTokenizer.resetSyntax();
            streamTokenizer.pushBack();
            streamTokenizer.wordChars(0, 255);
            for (char c: DEFAULT_DELIMITERS.toCharArray()) { streamTokenizer.whitespaceChars(c, c); }
            for (char c: DEFAULT_QUOTE_CHARS.toCharArray()) { streamTokenizer.quoteChar(c); }

            return readAll(streamTokenizer);

        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
    }

    private static List<String> readAll(StreamTokenizer streamTokenizer) throws IOException {
        List<String> tokens = new ArrayList<String>();
        int currentToken = 0;
        currentToken = streamTokenizer.nextToken();
        while (currentToken != StreamTokenizer.TT_EOF) {

            if (streamTokenizer.ttype == StreamTokenizer.TT_NUMBER) {
                tokens.add(""+ streamTokenizer.nval);
            } else if (streamTokenizer.ttype == StreamTokenizer.TT_WORD
                    || streamTokenizer.ttype == '\''
                    || streamTokenizer.ttype == '"') {
                tokens.add(streamTokenizer.sval);
            } else {
                tokens.add(""+(char) currentToken);
            }

            currentToken = streamTokenizer.nextToken();
        }

        return tokens;
    }

    String peekedNextToken = null;
    
    public synchronized boolean hasMoreTokens() {
        return peekedNextToken!=null;
    }
    
    public synchronized String nextToken() {    
        if (peekedNextToken==null) throw new NoSuchElementException();
        String lastToken = peekedNextToken;
        updateNextToken();
        return includeQuotes ? lastToken : expectQuotesDelimited ? unwrapIfQuoted(lastToken) : unquoteToken(lastToken);
    }

    /** returns true if the given string is one quoted string. if fail is specified, if it looks quoted but has unescaped internal quotes of the same type, it will fail. */
    public boolean isQuoted(String token) {
        if (Strings.isBlank(token)) return false;
        token = token.trim();
        if (!isQuoteChar(token.charAt(0))) return false;
        if (isUnterminatedQuotedPhrase(token)) return false;
        if (!token.endsWith(""+token.charAt(0))) return false;

        String quoteFreeRequired = token.substring(1, token.length() - 1);
        quoteFreeRequired = Strings.replaceAllRegex(quoteFreeRequired, "\\\\.", "");
        while (true) {
            int i = quoteFreeRequired.indexOf("" + token.charAt(0));
            if (i==-1) break;
            quoteFreeRequired = quoteFreeRequired.substring(i+1);
            if (quoteFreeRequired.isEmpty()) break;
            if (delimiters.contains(""+quoteFreeRequired.charAt(0))) return false;  // must not have " followed by delimeter
            // if fail is specified, quoted strings must escape their contents
            if (failOnOpenQuote) throw new IllegalArgumentException("Mismatched inner quotes");
        }

        return true;
    }

    public String unwrapIfQuoted(String token) {
        if (isQuoted(token)) {
            // unwrap outer
            token = token.substring(1, token.length()-1);
            // now unescape
            int i = 0;
            while ((i = token.indexOf('\\', i))>=0) {
                if (token.length() > i + 1) {
                    token = token.substring(0, i) + unescapeChar(token.charAt(i + 1)) + token.substring(i + 2);
                }
                i++;
            }
        }
        return token;
    }

    protected char unescapeChar(char c) {
        switch (c) {
            case '\\':
            case '\'':
            case '"':
                // above are supported literal escape chars
                return c;

            case 'n':
                return '\n';

            default:
                throw new IllegalArgumentException("Unsupported escape sequence \\"+c);
        }
    }

    /** this method removes all unescaped quote chars, i.e. quote chars preceded by no backslashes (or a larger even number of them);
     * it also unescapes '\\' as '\'.  it does no other unescaping.  */
    public String unquoteToken(String word) {
        // ( (\\A|[^\\\\]) (\\\\\\\\)* ) [ Pattern.quote(quoteChars) ]  $1
        word = word.replaceAll(
                "((\\A|[^\\\\])(\\\\\\\\)*)["+
                    //Pattern.quote(
                        quoteChars
                    //)
                        +"]+",
                "$1");
        //above pattern removes any quote preceded by even number of backslashes
        //now it is safe to replace any \c by c
        word = word.replaceAll("\\\\"+"([\\\\"+
                //Pattern.quote(
                quoteChars
                //)
                +"])", "$1");
                
        return word;
    }
    
    /** returns the input text escaped for use with unquoteTokens, and wrapped in the quoteChar[0] (usu a double quote) */
    public String quoteToken(String unescapedText) {
        String result = unescapedText;
        //replace every backslash by two backslashes
        result = result.replaceAll("\\\\", "\\\\\\\\");
        //now replace every quote char by backslash quote char
        result = result.replaceAll("(["+quoteChars+"])", "\\\\$1");
        //then wrap in quote
        result = quoteChars.charAt(0) + result + quoteChars.charAt(0);
        return result;
    }

    protected synchronized void updateNextToken() {
        peekedNextToken = null;
        String token;
        do {
            if (!delegate.hasMoreTokens()) return;
            token = delegate.nextToken();
            //skip delimiters
        } while (!includeDelimiters && token.matches("["+delimiters+"]+"));
        
        StringBuffer nextToken = new StringBuffer(token);
        pullUntilValid(nextToken);
        peekedNextToken = nextToken.toString();
    }

    private void pullUntilValid(StringBuffer nextToken) {
        while (isUnterminatedQuotedPhrase(nextToken.toString()) && delegate.hasMoreTokens()) {
            //keep appending until the quote is ended or there are no more quotes
            nextToken.append(delegate.nextToken());
        }
        if (failOnOpenQuote) {
            if (isUnterminatedQuotedPhrase(nextToken.toString())) {
                throw new IllegalArgumentException("Mismatched quotation marks in expression");
            }
            // do this for stricter error checks
            isQuoted(nextToken.toString());
        }
    }

    boolean isUnterminatedQuotedPhrase(String stringToCheck) {
        if (!expectQuotesDelimited) return hasOpenQuote(stringToCheck, quoteChars);

        if (stringToCheck.isEmpty()) return false;
        char start = stringToCheck.charAt(0);
        if (!isQuoteChar(start)) return false;
        // is open; does it also _end_ with an unescaped quote?
        int end = stringToCheck.length()-1;
        if (end==0) return true;
        char last = stringToCheck.charAt(end);
        if (!isQuoteChar(last)) return true;

        int numBackslashes = 0;
        while (stringToCheck.charAt(--end)=='\\') numBackslashes++;
        // odd number of backslashes means still open
        return numBackslashes % 2 == 1;
    }

    boolean isQuoteChar(char c) {
        return quoteChars.indexOf(c) >= 0;
    }

    public static boolean hasOpenQuote(String stringToCheck) {
        return hasOpenQuote(stringToCheck, DEFAULT_QUOTE_CHARS);
    }

    /** detects whether there are mismatched quotes, ignoring delimeters, but attending to escapes, finding a quote char, then skipping everything up to its match, and repeating */
    public static boolean hasOpenQuote(String stringToCheck, String quoteChars) {        
        String x = stringToCheck;
        if (x==null) return false;

        StringBuffer xi = new StringBuffer();
        for (int i=0; i<x.length(); i++) {
            char c = x.charAt(i);
            if (c=='\\') i++;
            else if (quoteChars.indexOf(c)>=0) {
                xi.append(c);
            }
        }
        x = xi.toString();
        
        while (x.length()>0) {
            char c = x.charAt(0);
            int match = x.indexOf(c, 1);
            if (match==-1) return true;
            x = x.substring(match+1);
        }
        return false;
    }

    public List<String> remainderAsList() {
        List<String> l = new ArrayList<String>();
        while (hasMoreTokens())
            l.add(nextToken());
        peekedNextToken = null;
        return l;
    }

    public List<String> remainderRaw() {
        List<String> l = new ArrayList<String>();
        if (peekedNextToken != null) l.add(peekedNextToken);
        while (delegate.hasMoreTokens())
            l.add(delegate.nextToken());
        peekedNextToken = null;
        return l;
    }

}
