JAVAC=javac
JAVA=java
CLASSPATH=.:lib/*
OUTDIR=out

SOURCEDIR=src

sources = $(wildcard $(SOURCEDIR)/**/StatusCode.java $(SOURCEDIR)/**/models/AttributeType.java $(SOURCEDIR)/**/models/AlgebraicOperator.java $(SOURCEDIR)/**/models/AssignmentExpression.java $(SOURCEDIR)/**/models/AssignmentOperator.java $(SOURCEDIR)/**/models/ComparisonOperator.java $(SOURCEDIR)/**/models/ComparisonPredicate.java $(SOURCEDIR)/**/models/IndexType.java $(SOURCEDIR)/**/models/IndexRecord.java $(SOURCEDIR)/**/models/NonClusteredBPTreeIndexRecord.java $(SOURCEDIR)/**/models/NonClusteredHashIndexRecord.java $(SOURCEDIR)/**/models/Record.java $(SOURCEDIR)/**/models/TableMetadata.java $(SOURCEDIR)/**/fdb/FDBKVPair.java $(SOURCEDIR)/**/fdb/FDBHelper.java $(SOURCEDIR)/**/Iterator.java $(SOURCEDIR)/**/RelationalAlgebraOperators.java $(SOURCEDIR)/**/RelationalAlgebraOperatorsImpl.java $(SOURCEDIR)/**/TableManager.java $(SOURCEDIR)/**/TableManagerImpl.java $(SOURCEDIR)/**/Cursor.java $(SOURCEDIR)/**/Indexes.java $(SOURCEDIR)/**/IndexesImpl.java $(SOURCEDIR)/**/IndexesTransformer.java $(SOURCEDIR)/**/Records.java $(SOURCEDIR)/**/RecordsImpl.java $(SOURCEDIR)/**/test/*.java)
classes = $(sources:.java=.class)

preparation: clean
	mkdir -p ${OUTDIR}

clean:
	rm -rf ${OUTDIR}

%.class: %.java
	$(JAVAC) -d "$(OUTDIR)" -cp "$(OUTDIR):$(CLASSPATH)" $<

part1Test: preparation $(classes)
	mkdir -p $(OUTDIR)
	$(JAVA) -cp "$(OUTDIR):$(CLASSPATH)" org.junit.runner.JUnitCore CSCI485ClassProject.test.Part1Test

part2Test: preparation $(classes)
	mkdir -p $(OUTDIR)
	$(JAVA) -cp "$(OUTDIR):$(CLASSPATH)" org.junit.runner.JUnitCore CSCI485ClassProject.test.Part2Test

.PHONY: part1Test part2Test clean preparation