import chalk from "chalk";

export function extract_text_from_brackets(str, first, last) {
  const start = str.indexOf(first) + first.length;
  const end = str.lastIndexOf(last) + 1 - last.length;

  if (start === -1 || end === -1) {
    return str;
  }

  return str.slice(start, end);
}

export function trim_end(str, char) {
  return str.replace(new RegExp(char + "*$"), "");
}

export function parse_json(str) {
  let json = {};

  try {
    json = JSON.parse(str);
  } catch (e) {
    console.log({ str });
    throw new Error("Failed to parse JSON " + e);
  }

  return json;
}

export const entitle = (str) =>
  console.log("\n" + chalk.bold.underline(str) + "\n");

export const print_test_result = (test) => {
  let chalk_grey = chalk.rgb(210, 220, 220);

  const func_type = chalk_grey(test.is_query ? "[query] " : "[update]");
  const passed_or_failed_test = test.result
    ? ` ✅ ${chalk.green(test.name)}`
    : ` ${chalk.bold.red("❌")} ${chalk.bold.red(test.name)}`;

  console.log(func_type + " " + passed_or_failed_test);

  for (const print_statement of test.print_log) {
    console.log("\t" + chalk_grey("➥  " + print_statement));
  }
};
