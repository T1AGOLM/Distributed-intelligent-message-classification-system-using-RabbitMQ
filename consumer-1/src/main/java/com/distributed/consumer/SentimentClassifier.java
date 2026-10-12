package com.distributed.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smile.classification.LogisticRegression;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Embedded sentiment-analysis AI (assignment task 6).
 *
 * <p>Pipeline: a labeled review corpus is loaded from
 * {@code sentiment-corpus.txt} (classpath), texts are converted to bag-of-words
 * feature vectors built from the corpus vocabulary, and a multinomial logistic
 * regression model trained with <b>Smile</b> classifies every vector into one
 * of three classes: {@code positive}, {@code negative} or {@code neutral}.</p>
 *
 * <p>The model is trained once at startup — no external service or network
 * call is involved at runtime.</p>
 */
public final class SentimentClassifier {

    private static final Logger log = LoggerFactory.getLogger(SentimentClassifier.class);

    /** Class labels indexed by the integer label the model predicts. */
    private static final String[] LABELS = {"negative", "neutral", "positive"};

    private static final String CORPUS = "/sentiment-corpus.txt";

    /** Words carrying no sentiment signal — dropped from the feature space. */
    private static final Set<String> STOPWORDS = Set.of(
            "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "from",
            "has", "have", "i", "in", "is", "it", "its", "me", "my", "of", "on",
            "or", "our", "so", "than", "that", "the", "their", "them", "there",
            "they", "this", "to", "too", "was", "we", "were", "with", "you",
            "your", "do", "does", "not", "no", "very", "just", "really", "all",
            "out", "up", "one", "when", "what", "which", "who", "am", "been",
            "being", "will", "can", "could", "would", "should", "had", "he", "she");

    private final Map<String, Integer> vocabulary;
    private final LogisticRegression.Multinomial model;

    private SentimentClassifier(Map<String, Integer> vocabulary,
                                 LogisticRegression.Multinomial model) {
        this.vocabulary = vocabulary;
        this.model = model;
    }

    /**
     * Loads the labeled corpus, learns the vocabulary, trains the Smile model
     * and returns the ready-to-use classifier.
     */
    public static SentimentClassifier train() throws Exception {
        List<String[]> samples = loadCorpus();

        // 1) Vocabulary learned from the corpus only (no leakage from inputs).
        Map<String, Integer> vocabulary = new LinkedHashMap<>();
        for (String[] sample : samples) {
            for (String token : tokenize(sample[1])) {
                vocabulary.putIfAbsent(token, vocabulary.size());
            }
        }

        // 2) Bag-of-words feature vectors (term counts, L2-normalized).
        double[][] x = new double[samples.size()][];
        int[] y = new int[samples.size()];
        for (int i = 0; i < samples.size(); i++) {
            x[i] = vectorize(vocabulary, samples.get(i)[1]);
            y[i] = labelIndex(samples.get(i)[0]);
        }

        // 3) Train the multinomial logistic regression with Smile.
        LogisticRegression.Multinomial trained = LogisticRegression.multinomial(x, y);

        log.info("Smile sentiment model trained: {} samples, {} features, classes={}",
                samples.size(), vocabulary.size(), List.of(LABELS));

        return new SentimentClassifier(vocabulary, trained);
    }

    /** Classifies a free-text review as {@code positive}, {@code negative} or {@code neutral}. */
    public String classify(String text) {
        return LABELS[model.predict(vectorize(vocabulary, text))];
    }

    // --- helpers -------------------------------------------------------------

    private static List<String[]> loadCorpus() throws Exception {
        List<String[]> samples = new ArrayList<>();
        try (InputStream in = SentimentClassifier.class.getResourceAsStream(CORPUS)) {
            if (in == null) {
                throw new IllegalStateException("Corpus resource not found: " + CORPUS);
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                String[] parts = line.split("\\|", 2);
                if (parts.length != 2 || labelIndex(parts[0]) < 0) {
                    throw new IllegalStateException("Invalid corpus line: " + line);
                }
                samples.add(new String[]{parts[0], parts[1]});
            }
        }
        return samples;
    }

    private static int labelIndex(String label) {
        for (int i = 0; i < LABELS.length; i++) {
            if (LABELS[i].equals(label)) {
                return i;
            }
        }
        return -1;
    }

    /** Lowercases, strips accents-ish punctuation and splits into tokens. */
    private static List<String> tokenize(String text) {
        String[] raw = text.toLowerCase(Locale.ROOT).split("[^a-z0-9]+");
        List<String> tokens = new ArrayList<>();
        for (String token : raw) {
            if (token.length() > 1 && !STOPWORDS.contains(token)) {
                tokens.add(token);
            }
        }
        return tokens;
    }

    /** Builds the bag-of-words vector of a text, L2-normalized. */
    private static double[] vectorize(Map<String, Integer> vocabulary, String text) {
        double[] vector = new double[vocabulary.size()];
        for (String token : tokenize(text)) {
            Integer index = vocabulary.get(token);
            if (index != null) {
                vector[index] += 1.0;
            }
        }
        double norm = 0.0;
        for (double value : vector) {
            norm += value * value;
        }
        if (norm > 0) {
            double invSqrt = 1.0 / Math.sqrt(norm);
            for (int i = 0; i < vector.length; i++) {
                vector[i] *= invSqrt;
            }
        }
        return vector;
    }
}
