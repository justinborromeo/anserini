/*
 * Anserini: A Lucene toolkit for replicable information retrieval research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.anserini.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A sentence collection for the Epidemic QA dataset (https://bionlp.nlm.nih.gov/epic_qa/).
 */
public class EpidemicQASentenceCollection extends DocumentCollection<EpidemicQASentenceCollection.Document> {
    private static final Logger LOG = LogManager.getLogger(EpidemicQASentenceCollection.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public EpidemicQASentenceCollection(Path path){
        this.path = path;
        // Documents are stored in JSON files (1 document/JSON file).
        this.allowedFileSuffix = Set.of(".json");
    }

    @Override
    public FileSegment<EpidemicQASentenceCollection.Document> createFileSegment(Path p) throws IOException {
        return new Segment(p);
    }

    /**
    * A single JSON file containing a document.
    */
    public class Segment extends FileSegment<EpidemicQASentenceCollection.Document> {
        private EpidemicQASentenceCollection.Document curr;
        private Iterator<EpidemicQASentenceCollection.Document> iterator = null;

        public Segment(Path path) throws IOException {
            super(path);
            this.bufferedReader = new BufferedReader(new InputStreamReader(
                new FileInputStream(path.toString())));
            LOG.info("Path: " + path.toString());

            String content = new String(Files.readAllBytes(path));
            JsonNode fullTextJson = mapper.readTree(content);
            
            List<EpidemicQASentenceCollection.Document> sentenceDocuments = new ArrayList<>();
            Iterator<JsonNode> contextsIterator = fullTextJson.get("contexts").elements();
            while(contextsIterator.hasNext()) {
                JsonNode contextNode = contextsIterator.next();
                String contextText = contextNode.get("text").asText();
                Iterator<JsonNode> sentencesIterator = contextNode.get("sentences").elements();
                while(sentencesIterator.hasNext()) {
                    JsonNode sentenceNode = sentencesIterator.next();
                    int startIndex = sentenceNode.get("start").asInt();
                    int endIndex = sentenceNode.get("end").asInt();
                    String sentenceId = sentenceNode.get("sentence_id").asText();
                    String sentenceText = contextText.substring(startIndex, endIndex);
                    EpidemicQASentenceCollection.Document sentenceDoc = 
                        new Document(sentenceId, sentenceText, sentenceText);
                    sentenceDocuments.add(sentenceDoc);
                }
            }

            iterator = sentenceDocuments.listIterator();
            if(iterator.hasNext()) {
                curr = iterator.next();
            }
        }

        @Override
        public void readNext() throws NoSuchElementException {
            bufferedRecord = curr;
            if (iterator.hasNext()) {
                curr = iterator.next();
            } else {
                atEOF = true;
            }
        }

        @Override
        public void close() {
            super.close();
        }
    }

    /**
    * A class that maps to one of the Epidemic QA JSON documents' sentences.
    */
    public class Document implements SourceDocument {
        protected String id;
        protected String content;
        protected String raw;

        public Document(String id, String content, String raw) {
            this.id = id;
            this.content = content;
            this.raw = raw;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public String contents() {
            return content;
        }

        @Override
        public String raw() {
            return raw;
        }

        @Override
        public boolean indexable() {
            return true;
        }
    }
}
