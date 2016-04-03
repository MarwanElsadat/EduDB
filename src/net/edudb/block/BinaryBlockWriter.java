package net.edudb.block;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import net.edudb.engine.Config;
import net.edudb.page.Pageable;

public class BinaryBlockWriter implements BlockWriter {

	@Override
	public void write(Pageable page) throws IOException {
		FileOutputStream fileOut = new FileOutputStream(Config.pagesPath() + page.getName() + ".block");
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(page);
		out.close();
		fileOut.close();
	}

}
