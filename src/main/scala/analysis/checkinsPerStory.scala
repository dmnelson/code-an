package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


class CheckinsPerStoryAnalyzer extends Analyzer {
    def apply(data: RepoData, sc: SparkContext) = {
        Result(Seq("story", "checkins", "added", "removed", "project"),
            data.commits
                .flatMap(c => c.files.map(f => (c.story, c.project, c.hash, f.added, f.removed)))
                .groupBy{ case(story, project,_, _, _) => (story, project)}
                .map { case ((story, project), changes) =>
                    val count = changes.map{ case(_,_, hash,_, _) => hash}.distinct.length
                    val (added: Int, removed: Int) = changes.foldLeft(0,0){
                      case ((sumAdded, sumRemoved), (_, _, _, added, removed)) => 
                        (sumAdded + added, sumRemoved + removed) 
                    }
                    (story, project, count, added, removed)
                }
                .toArray
                .sortBy { case (_, _, count, _, _) => -count }
                .map { case (story, project, count, added, removed) =>
                List(story.getOrElse("-unknown-"), count, added, removed, project) })
    }

}

