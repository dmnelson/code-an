package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._

class TestLinesVsCodeLinesPerProjectAnalyzer extends Analyzer {

    def apply(data: RepoData, sc: SparkContext) = {

        val codeExtensions = List("java", "rb", "js", "htm", "html", "jsp", "feature")

        Result(Seq("project", "test", "regular"),
            data.commits
            .flatMap(c => c.files.filter(_.isOfType(codeExtensions)).map(f => (c.project, f.added -
              f.removed, f.isTest)))
            .groupBy { case (project, added, isTest) => project }
            .map { case (project, changes) =>
                val (test, regular) = changes
                    .foldLeft((0,0)) { case ((totalTest, totalRegular), (project, added, isTest)) =>
                        (if(isTest) totalTest + added else totalTest, if (!isTest) totalRegular + added else totalRegular)
                    }

                (project, test, regular)
            }
            .toArray
            .map(_.productIterator.toList))
    }

}
