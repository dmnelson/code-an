package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._

class TestLinesVsCodeLinesPerAuthorAnalyzer extends Analyzer {

    def apply(data: RepoData, sc: SparkContext) =
        Result(Seq("author", "test", "regular"),
            data.commits
            .flatMap(c => c.files.map(f => (c.author, f.added, f.isTest)))
            .groupBy { case (author, added, isTest) => author }
            .map { case (author, changes) =>
                val (test, regular) = changes
                    .foldLeft((0,0)) { case ((totalTest, totalRegular), (author, added, isTest)) =>
                        (if(isTest) totalTest + added else totalTest, if (!isTest) totalRegular + added else totalRegular)
                    }

                (author, test, regular)
            }
            .toArray
            .map(_.productIterator.toList))

}