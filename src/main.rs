use anyhow;
use dot_writer::{Attributes, DotWriter, Shape, Style};
use itertools::Itertools; // for join on hashset
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::io;

#[derive(Debug, Deserialize)]
struct ConceptRecord {
    concept: String,
    dependencies: String,
    category: Option<String>,
    week: Option<u64>,
    earliest: Option<u64>,
    latest: Option<u64>,
    #[serde(rename = "lecture weight")]
    lecture_weight: Option<f64>,
    #[serde(rename = "lab weight")]
    lab_weight: Option<f64>,
    #[serde(rename = "hw weight")]
    hw_weight: Option<f64>,
    #[serde(rename = "lecture coverage")]
    lecture_coverage: Option<f64>,
    #[serde(rename = "lab coverage")]
    lab_coverage: Option<f64>,
    #[serde(rename = "hw coverage")]
    hw_coverage: Option<f64>,
}

type ConceptName = String;

#[derive(Debug)]
struct ConceptMap {
    nconcepts: usize,
    concepts: Vec<Concept>,
    lookup: HashMap<ConceptName, usize>, // mapping to offsets into `concepts`
    dependency_order: Vec<ConceptName>,
    errors: String,
    total_weights: [f64; 3],
}

impl ConceptMap {
    fn new() -> Self {
        ConceptMap {
            nconcepts: 0, // just tracking the line number
            concepts: Vec::new(),
            lookup: HashMap::new(),
            dependency_order: Vec::new(),
            errors: String::from(""),
            total_weights: [0.0, 0.0, 0.0],
        }
    }

    fn errs(&self) -> Option<String> {
        if self.errors == String::from("") {
            None
        } else {
            Some(self.errors.clone())
        }
    }

    fn dependency_to_concept(&self, n: &ConceptName) -> &Concept {
        let idx = *self.lookup.get(n).unwrap(); // we've already validated all entries
        &self.concepts[idx]
    }

    fn solve_dependency_transitive_closure(
        &self,
        mut dep_closure: &mut HashMap<ConceptName, HashSet<ConceptName>>,
        n: &ConceptName,
    ) {
        // Base case #1: Have we already solved this component's
        // dependency transitive closure? This is the memoization
        // logic.
        if let Some(_) = dep_closure.get(n) {
            return;
        }
        let ds = &self.dependency_to_concept(n).dependencies;

        // Base case #2: do we have a concept with no dependencies?
        if ds.len() == 0 {
            let deps: HashSet<ConceptName> = ds.iter().cloned().collect();
            // unwrap: we know there is no previous value due to the check above.
            dep_closure.insert(n.to_string(), deps);

            return;
        }

        // Recursive case: Aggregate the union of our dependency's
        // transitive closures.
        let mut all: HashSet<ConceptName> = ds.iter().cloned().collect();
        for d in ds {
            self.solve_dependency_transitive_closure(&mut dep_closure, &d);

            let tc = dep_closure.get(d).unwrap(); // just inserted!
            all = all.union(tc).cloned().collect();
        }
        dep_closure.insert(n.to_string(), all);
    }

    fn graph(&self) -> Vec<u8> {
        let mut output_bytes = Vec::new();
        {
            let mut writer = DotWriter::from(&mut output_bytes);
            let mut digraph = writer.digraph();
            let mut colors = HashMap::new();
            let colormap = [
                "cadetblue1",
                "chocolate1",
                "darkgoldenrod1",
                "darkorchid1",
                "deeppink",
                "dodgerblue2",
                "firebrick1",
                "gray38",
                "green3",
                "navy",
                "orchid",
                "teal",
                "violetred",
                "yellow1",
                "tomato1",
            ];
            let mut color_idx = 0;

            digraph
                .node_attributes()
                .set("penwidth", "2.5", false);
            for c in &self.concepts {
                if !colors.contains_key(&c.category) {
                    assert_ne!(color_idx, colormap.len()); // don't support more than this many categories
                    colors.insert(&c.category, &colormap[color_idx]);
                    color_idx += 1;
                }
            }
            for c in &self.concepts {
                digraph.node_named(c.graph_name.to_string()).set(
                    "color",
                    &colors.get(&c.category).unwrap().to_string(),
                    true,
                );
                // unwrap: added in previous loop
            }
            let summary_name = format!(
                "\"Summary\nLecture {:.2} weeks\nLab {:.2} weeks\nHW {:.2} weeks\"",
                self.total_weights[0], self.total_weights[1], self.total_weights[2]
            );
            digraph
                .node_named(summary_name.to_string())
                .set_shape(Shape::None)
                .set_font_size(20.0);

            for (cat, col) in &colors {
                digraph
                    .node_named(cat.to_string())
		    .set_style(Style::Filled)
                    .set_shape(Shape::Rectangle)
                    .set("color", col, true);
            }
            self.dependency_order
                .iter()
                .map(|o| self.dependency_to_concept(o))
                .for_each(|c| {
                    for d in &c.dependencies {
                        let dep_c = self.dependency_to_concept(d);

                        digraph.edge(c.graph_name.to_string(), dep_c.graph_name.to_string());
                    }
                });
        }
        output_bytes
    }

    fn solve_total_weights(&mut self) {
        for i in 0..3 {
            self.total_weights[i] = self.concepts.iter().map(|c| c.modes[i].weight).sum();
        }
    }

    fn solve(&mut self) -> String {
        let mut all_deps: HashMap<ConceptName, HashSet<ConceptName>> = HashMap::new();
        let mut weights: HashMap<ConceptName, f64> = HashMap::new();

        self.solve_total_weights();

        for c in &self.concepts {
            self.solve_dependency_transitive_closure(&mut all_deps, &c.concept);
            weights.insert(c.concept.clone(), c.modes[0].weight);
        }

        for c in &mut self.concepts {
            let all = all_deps.get(&c.concept).unwrap(); // just inserted!
            c.modes[0].range.earliest_start = all
                .iter()
                .map(|d| weights.get(d).unwrap()) // added in previous loop
                .fold(0.0, |p, n| p + n);
            c.graph_name = format!(
                "\"{}\nearliest: {:.2}\"",
                c.concept, c.modes[0].range.earliest_start
            );
        }

        String::from_utf8_lossy(&self.graph()).to_string()
    }
}

struct ConceptMapBuilder {
    map: ConceptMap,
}

impl ConceptMapBuilder {
    fn new() -> Self {
        ConceptMapBuilder {
            map: ConceptMap::new(),
        }
    }

    fn add(&mut self, c: ConceptRecord) {
        let map = &mut self.map;
        map.nconcepts = map.nconcepts + 1;
        if let Some(&redundant) = map.lookup.get(&c.concept) {
            map.errors.push_str(format!(
                "- Found redundant copy of concept \"{}\" in record {} (redundant with record {}). Ignoring concept entry.\n",
                c.concept, map.nconcepts, map.concepts[redundant].line
            ).as_str());
            return;
        }

        let mut concept = Concept::new(&c, map.nconcepts + 1);
        let mut deps: Vec<ConceptName> = Vec::new();
        for d in c.dependencies.split(";") {
            // Note: we cannot check if the dependency is a valid
            // concept yet, as it could be in concepts added later.
            // See the validation after all concepts are added.
            if d != "" {
                deps.push(d.trim().to_string());
            }
        }

        concept.add_dependencies(deps);
        let offset = map.concepts.len(); // where are we adding ourselves into the vector?
        concept.add_offset(offset);
        map.lookup.insert(concept.concept.clone(), offset);
        let name = concept.concept.clone();
        map.concepts.push(concept);
        assert_eq!(map.concepts[offset].concept, name);

        return;
    }

    fn validate(&mut self) {
        let mut errs = String::from("");
        let m = &mut self.map;

        for mut c in &mut m.concepts {
            let mut ds = Vec::new();

            for d in &c.dependencies {
                if m.lookup.get(d).is_none() {
                    errs.push_str(format!("- Dependency on \"{}\" in concept \"{}\" in record {} does not correspond to a concept. Ignoring dependency.\n", d, c.concept, c.line).as_str());
                } else {
                    ds.push(d.clone());
                }
            }
            c.dependencies = ds;
        }

        // simple cycle detection
        let mut pending = HashSet::new();

        for c in &m.concepts {
            pending.insert(c.concept.clone());
        }
        loop {
            let mut shrunk = false;

            for c in &m.concepts {
                let mut on_frontier = true;

                if !pending.contains(&c.concept) {
                    continue;
                }

                for d in &c.dependencies {
                    if pending.contains(d) {
                        on_frontier = false;
                        break;
                    }
                }

                if on_frontier {
                    m.dependency_order.push(c.concept.clone());
                    pending.remove(&c.concept);
                    shrunk = true;
                }
            }

            if !shrunk || pending.len() == 0 {
                break;
            }
        }
        if pending.len() > 0 {
            errs.push_str(
                format!(
                    "- Circular conceptual dependencies including (or depended on by) {} concepts: {}.\n",
                    pending.len(),
                    pending.iter().join(", ")
                )
                .as_str(),
            );
        }

        // record errors
        m.errors.push_str(&mut errs);
    }

    fn build(mut self) -> ConceptMap {
        self.validate();
        self.map
    }
}

#[derive(Debug)]
struct TimeRange {
    start: f64,
    earliest_start: f64,
    latest_end: f64,
}

#[derive(Debug)]
struct Modality {
    range: TimeRange,
    weight: f64,
    coverage: f64,
}

impl Modality {
    fn new(weight: f64, r: Option<TimeRange>) -> Self {
        Modality {
            range: r.unwrap_or(TimeRange {
                start: 0.0,
                earliest_start: 0.0,
                latest_end: 0.0,
            }),
            weight: weight,
            coverage: 0.0,
        }
    }
}

#[derive(Debug)]
struct Concept {
    concept: ConceptName,
    category: String,
    line: usize,
    offset: usize,
    dependencies: Vec<ConceptName>,
    modes: [Modality; 3],
    graph_name: String,
}

impl Concept {
    pub fn new(r: &ConceptRecord, line: usize) -> Self {
        Concept {
            concept: r.concept.clone().trim().to_string(),
            category: r
                .category
                .clone()
                .unwrap_or(String::from(""))
                .trim()
                .to_string(),
            line,
            offset: 0,
            dependencies: Vec::new(),
            modes: [
                Modality::new(r.lecture_weight.unwrap_or(0.0), None),
                Modality::new(r.lab_weight.unwrap_or(0.0), None),
                Modality::new(r.hw_weight.unwrap_or(0.0), None),
            ],
            graph_name: String::from(""),
        }
    }

    pub fn add_dependencies(&mut self, deps: Vec<ConceptName>) {
        self.dependencies = deps;
    }

    fn add_offset(&mut self, offset: usize) {
        self.offset = offset;
    }
}

fn main() -> anyhow::Result<()> {
    let mut rdr = csv::Reader::from_reader(io::stdin());
    let mut mb = ConceptMapBuilder::new();

    for entry in rdr.deserialize() {
        let concept: ConceptRecord = entry?;

        mb.add(concept);
    }

    let mut m = mb.build();

    if let Some(es) = m.errs() {
        eprint!("Errors in csv file:\n{}", es);
    }

    println!("{}", m.solve());

    Ok(())
}
